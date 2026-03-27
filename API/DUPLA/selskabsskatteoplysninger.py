import os
import dotenv
import requests
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4
from typing import Callable, Optional
from API.DUPLA.dupla import Dupla
from Database.db import SQL
from datetime import datetime
from API.http_logging import (
    extract_error_code,
    read_float_env,
    read_int_env,
)
from logger import build_logger_service

dotenv.load_dotenv()

LOGGER = build_logger_service()


def _new_log_context(*, correlation_id: str | None = None, robot_id: str | None = None):
    """Create a shared logging context for DUPLA flows."""
    return LOGGER.new_context(
        correlation_id=correlation_id,
        source_system="DUPLA",
        script_name="API.DUPLA.selskabsskatteoplysninger",
        robot_id=robot_id,
    )


def _rate_limit_delay() -> float:
    """Read and normalize RATE_LIMITING from environment to a float delay."""
    return read_float_env("RATE_LIMITING", 0.2)


class SelskabsskatteoplysningerService:
    """
    Service for retrieving and storing DUPLA selskabsselvangivelser data.

    Attributes:
        auth (Dupla): Authenticated DUPLA client context.
        sql (SQL): Database helper used for persistence.
        onbehalfofse (str): Active company SE number used in API filtering.
        uuid (str): Service instance identifier.
        year (int): Current calendar year snapshot.
    """

    def __init__(self, auth: Dupla, sql: SQL):
        """
        Initialize a selskabsselvangivelser service instance.

        Args:
            auth (Dupla): DUPLA authentication context.
            sql (SQL): SQL helper instance used for data persistence.
        """
        self.auth = auth
        self.sql = sql
        self.onbehalfofse = auth.onbehalfofse
        self.uuid = str(uuid4())
        self.year = datetime.now().year

    def fetch_selskabselvangivelse(self, year: int, cvr: str, should_cancel: Optional[Callable[[], bool]] = None):
        """
        Fetch one company's annual selskabsskatteoplysning payload from DUPLA.

        Args:
            year (int): Report year to request.
            cvr (str): Company CVR number to query.
            should_cancel (Optional[Callable[[], bool]], optional): Optional cancellation predicate.

        Returns:
            tuple[dict, str]: Raw API payload and transaction id used for logging.
        """
        url = f"{os.getenv('DUPLA_BASE_URL')}/Selskabsskatteoplysninger/Selskabselvangivelse"
        trans_id = str(uuid4())

        if not cvr:
            cvr = self.onbehalfofse

        def get_headers(request_trans_id: str):
            return {
                "Authorization": f"Bearer {self.auth.access_token}",
                "Content-Type": "application/json",
                "X-Transaktions-ID": request_trans_id,
                "X-Request-ID": str(uuid4()),
                "UFST-Adgangsgrundlag": os.getenv("DUPLA_ADGANGSGRUNDLAG"),
            }

        all_data = []
        next_url = url
        params = {
            "VirksomhedSENummer": cvr,
            "SelskabSelvangivelseIndkomstÃ…r": year,
        }

        while next_url:
            if should_cancel and should_cancel():
                break

            current_trans_id = str(uuid4())
            log_context = _new_log_context(correlation_id=current_trans_id)
            headers = get_headers(current_trans_id)

            try:
                if next_url == url:
                    response = requests.get(
                        next_url,
                        headers=headers,
                        cert=self.auth.cert,
                        params=params,
                        verify=True,
                    )
                else:
                    response = requests.get(
                        next_url,
                        headers=headers,
                        cert=self.auth.cert,
                        verify=True,
                    )
            except requests.RequestException as exc:
                LOGGER.log_request_exception(
                    f"DUPLA_Selvangivelse_RequestError_{cvr}",
                    current_trans_id,
                    exc,
                    method="GET",
                    url=next_url,
                    request_headers=headers,
                    request_params=params if next_url == url else None,
                    context=log_context,
                )
                raise

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                LOGGER.log_request_failure(
                    f"DUPLA_Selvangivelse_Failed_{cvr}",
                    current_trans_id,
                    response=response,
                    method="GET",
                    url=next_url,
                    request_headers=headers,
                    request_params=params if next_url == url else None,
                    context=log_context,
                    message=str(exc),
                )
                raise requests.HTTPError(f"{exc}. Response body: {response.text}") from exc

            page_data = response.json()
            LOGGER.log_request_success(
                "DUPLA_Selvangivelse_Request",
                current_trans_id,
                response=response,
                method="GET",
                url=next_url,
                request_headers=headers,
                request_params=params if next_url == url else None,
                context=log_context,
            )
            all_data.extend(page_data.get("data", []))

            next_url = page_data.get("links", {}).get("next")
            if next_url:
                time.sleep(_rate_limit_delay())

        return {"data": all_data}, trans_id

    def save_selskabselvangivelse_response(
        self,
        payload: dict,
        correlation_id: str,
        should_cancel: Optional[Callable[[], bool]] = None,
    ):
        """
        Persist DUPLA selskabsselvangivelser payload to the normalized schema table.

        Args:
            payload (dict): DUPLA API response payload containing selskabsselvangivelser data.
            correlation_id (str): Correlation id used for transaction logging.
            should_cancel (Optional[Callable[[], bool]], optional): Callback that returns True when cancellation is requested.
        """
        if "data" not in payload:
            raise ValueError("Invalid payload: 'data' key not found")

        transaction_id = LOGGER.log_transaction(
            "DUPLA_Selvangivelse",
            correlation_id,
            code="200",
            context=_new_log_context(correlation_id=correlation_id),
        )
        processed = 0

        for item in payload["data"]:
            if should_cancel and should_cancel():
                break
            attr = item.get("attributes", {})
            selvangivelse_oplysninger = attr.get("SelvangivelseOplysninger", {})
            teknisk_registrering_dato = selvangivelse_oplysninger.get("TekniskRegistreringDato") or ""
            teknisk_registrering_aar = (
                str(teknisk_registrering_dato).split("-", 1)[0]
                if teknisk_registrering_dato
                else ""
            )

            selvangivelse_pk = f"{self.onbehalfofse}_{teknisk_registrering_aar}"
            selvangivelse = {
                "id": selvangivelse_pk,
                "CustomerId": self.onbehalfofse,
                "TekniskRegistreringDato": selvangivelse_oplysninger.get("TekniskRegistreringDato"),
                "IndkomstÃ…r": selvangivelse_oplysninger.get("SelskabSelvangivelseIndkomstÃ…r"),
                "DelperiodeStart": selvangivelse_oplysninger.get("SelvangivelseDelperiodeStart"),
                "DelperiodeSlut": selvangivelse_oplysninger.get("SelvangivelseDelperiodeSlut"),
                "OpdateretTimestamp": selvangivelse_oplysninger.get("SelskabSelvangivelseOpdateretTimestamp"),
            }

            self.sql.batch_upsert("selskabsskat.Selvangivelse", [selvangivelse], pk=["id"], return_ids=False)
            processed += 1

        return {
            "transaction_id": transaction_id,
            "records_processed": processed,
        }

    def fetch_all_selskabsselvangivelser(
        *,
        year: int | None = None,
        cvr: str | None = None,
        should_cancel: Optional[Callable[[], bool]] = None,
    ):
        """
        Retrieve and persist selskabsselvangivelse data for all eligible customers.

        Args:
            year (int, optional): Target year. Uses current year when omitted.
            cvr (str, optional): Reserved argument for API compatibility.

        Returns:
            None
        """
        if year is None:
            year = datetime.now().year

        run_context = _new_log_context(robot_id="selskabsselvangivelse_all")
        run = LOGGER.create_run(
            robot_id="selskabsselvangivelse_all",
            trigger_source="cli",
            input_params={"year": year, "cvr": cvr},
            context=run_context,
        )
        run.started()

        max_workers = read_int_env("SELSKABSSKATTEOPLYSNINGER_MAX_WORKERS", 10, min_value=1)
        semaphore = threading.BoundedSemaphore(value=max_workers)

        sql = SQL()
        batches = sql.get_data(
            "tv.Customer",
            selects=["CVR"],
            where_clause="LEN(CVR) >= 8",
            batch_size=25,
        )

        cvrs: list[str] = []
        for batch in batches:
            if should_cancel and should_cancel():
                run.cancelled("Cancelled before DUPLA full sync completed")
                return
            for (cvr_value,) in batch:
                cvr_str = str(cvr_value)
                if len(cvr_str) >= 8:
                    cvrs.append(cvr_str)

        def process_one_cvr(cvr_value: str) -> dict[str, object]:
            corr_id = str(uuid4())
            if should_cancel and should_cancel():
                return {"success": False, "cancelled": True, "correlation_id": corr_id, "records_processed": 0}

            with semaphore:
                try:
                    worker_sql = SQL()
                    auth = Dupla(onbehalfofse=cvr_value)
                    service = SelskabsskatteoplysningerService(auth, worker_sql)

                    payload, corr_id = service.fetch_selskabselvangivelse(year, cvr_value, should_cancel=should_cancel)
                    if should_cancel and should_cancel():
                        return {"success": False, "cancelled": True, "correlation_id": corr_id, "records_processed": 0}
                    save_result = service.save_selskabselvangivelse_response(payload, corr_id, should_cancel=should_cancel)

                    time.sleep(_rate_limit_delay())
                    return {
                        "success": True,
                        "correlation_id": corr_id,
                        "transaction_id": save_result["transaction_id"],
                        "records_processed": save_result["records_processed"],
                    }
                except Exception as exc:
                    error_code = extract_error_code(exc.response) if isinstance(exc, requests.HTTPError) else 400
                    LOGGER.log_transaction(
                        f"DUPLA_Selvangivelse_Failed_{cvr_value}",
                        corr_id,
                        code=error_code,
                        message=str(exc),
                    )
                    time.sleep(_rate_limit_delay())
                    return {
                        "success": False,
                        "correlation_id": corr_id,
                        "records_processed": 0,
                    }

        total_processed = 0
        total_succeeded = 0
        total_failed = 0
        latest_transaction_id = None
        latest_correlation_id = run_context.correlation_id

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for cvr_value in cvrs:
                if should_cancel and should_cancel():
                    run.cancelled("Cancelled before DUPLA full sync submitted all tasks")
                    break
                futures.append(pool.submit(process_one_cvr, cvr_value))

            for future in as_completed(futures):
                if should_cancel and should_cancel():
                    for pending in futures:
                        pending.cancel()
                    run.cancelled("Cancelled during DUPLA full sync")
                    break
                result = future.result()
                if result.get("cancelled"):
                    continue
                total_processed += int(result.get("records_processed", 0))
                latest_correlation_id = str(result.get("correlation_id") or latest_correlation_id)
                if result.get("success"):
                    total_succeeded += 1
                    latest_transaction_id = result.get("transaction_id") or latest_transaction_id
                else:
                    total_failed += 1
                run.progress(
                    current=total_succeeded + total_failed,
                    total=len(cvrs),
                    message=f"Processed {total_succeeded + total_failed} of {len(cvrs)} CVRs",
                    records_processed=total_processed,
                    records_succeeded=total_succeeded,
                    records_failed=total_failed,
                )

        if should_cancel and should_cancel():
            return

        run.completed(
            correlation_id=latest_correlation_id,
            latest_transaction_id=latest_transaction_id,
            records_processed=total_processed,
            records_succeeded=total_succeeded,
            records_failed=total_failed,
        )

    def fetch_one_selskabsselvangivelse(
        *,
        cvr: str,
        year: int | None = None,
        should_cancel: Optional[Callable[[], bool]] = None,
    ):
        """
        Retrieve and persist selvskabsselvangivelse data for one CVR.

        Args:
            cvr (str): CVR number to process.
            year (int, optional): Target year. Uses current year when omitted.

        Raises:
            Exception: If API or database operations fail.

        Returns:
            None
        """
        if year is None:
            year = datetime.now().year
        if should_cancel and should_cancel():
            return

        sql = SQL()
        run_context = _new_log_context(robot_id="selskabsselvangivelse_single")
        run = LOGGER.create_run(
            robot_id="selskabsselvangivelse_single",
            trigger_source="cli",
            input_params={"year": year, "cvr": cvr},
            context=run_context,
        )
        run.started()

        if len(cvr) >= 8:
            corr_id = str(uuid4())
            try:
                auth = Dupla(onbehalfofse=str(cvr))
                service = SelskabsskatteoplysningerService(auth, sql)

                payload, corr_id = service.fetch_selskabselvangivelse(year, cvr, should_cancel=should_cancel)
                if should_cancel and should_cancel():
                    run.cancelled("Cancelled during DUPLA single sync")
                    return
                save_result = service.save_selskabselvangivelse_response(payload, corr_id, should_cancel=should_cancel)
                run.attach_transaction(
                    correlation_id=corr_id,
                    latest_transaction_id=save_result["transaction_id"],
                )
                run.completed(
                    correlation_id=corr_id,
                    latest_transaction_id=save_result["transaction_id"],
                    records_processed=save_result["records_processed"],
                    records_succeeded=save_result["records_processed"],
                    records_failed=0,
                )
            except Exception as exc:
                error_code = extract_error_code(exc.response) if isinstance(exc, requests.HTTPError) else 400
                LOGGER.log_transaction(
                    f"DUPLA_Selvangivelse_Failed_{cvr}",
                    corr_id,
                    code=error_code,
                    message=str(exc),
                )
                run.failed(
                    error_message=str(exc),
                    failure_category="http" if isinstance(exc, requests.RequestException) else "processing",
                    correlation_id=corr_id,
                )
                raise
