"""
CVR API integration and persistence workflows.

This module retrieves accounting records from CVR and stores them in SQL tables.
"""

import requests
from requests.auth import HTTPBasicAuth
import os
import uuid
import time
import dotenv
import sys
import hashlib
from typing import Callable, Optional
from Database.db import SQL
from datetime import datetime, timezone, timedelta
from API.http_logging import extract_error_code
from logger import build_logger_service

dotenv.load_dotenv()

LOGGER = build_logger_service()


def _new_log_context(*, correlation_id: str | None = None, robot_id: str | None = None):
    """Create a shared logging context for CVR flows."""
    return LOGGER.new_context(
        correlation_id=correlation_id,
        source_system="CVR",
        script_name="API.CVR.cvr",
        robot_id=robot_id,
    )


def _resolve_log_code(exc: Exception) -> str:
    """Map CVR failures to the shared API log code format."""
    if isinstance(exc, requests.RequestException):
        return extract_error_code(getattr(exc, "response", None))
    return "400"

class CVRAPI:
    """
    Client wrapper for CVR API operations and related persistence logic.

    Attributes:
        BASE_URL (str): Base URL for CVR API requests.
        api_key (str): API key value provided at initialization.
        transaction_id (str): Correlation id used for API logging.
        headers (dict): Default HTTP headers for API calls.
        username (str): Basic-auth username from environment configuration.
        password (str): Basic-auth password from environment configuration.
    """

    BASE_URL = os.getenv('CVR_API_URL')
    
    def __init__(self, api_key: str):
        """
        Initialize a CVR API client.

        Args:
            api_key (str): CVR API key.
        """
        self.api_key = api_key
        self.transaction_id = str(uuid.uuid4())
        self.headers = {
            "Content-Type": "application/json",
        }
        self.username = os.getenv('CVR_API_USERNAME')
        self.password = os.getenv('CVR_API_PASSWORD')

    def _request(self, method: str, body: list, start: str, end: str, **kwargs):
        """
        Execute a low-level CVR API request for one fiscal period.

        Args:
            method (str): HTTP method to use (typically `POST`).
            body (list): CVR numbers to include in the `terms` filter.
            start (str): Fiscal period start date (`YYYY-MM-DD`).
            end (str): Fiscal period end date (`YYYY-MM-DD`).
            **kwargs: Additional `requests.request` keyword arguments.

        Returns:
            tuple: `(response_json, transaction_id)`.

        Raises:
            requests.HTTPError: If the API response status indicates an error.
        """
        url = f"{self.BASE_URL}"
        json = {
            "size": 3000,
            "query":{
                "bool":{
                    "filter":[
                        {"terms": {
                            "cvrNummer": [item for item in body]
                        }},
                        {"term": {
                            "regnskab.regnskabsperiode.startDato": start
                        }},
                        {"term": {
                            "regnskab.regnskabsperiode.slutDato": end
                        }}
                    ]
                    }
                },
                "_source": [
                    "cvrNummer",
                    "offentliggoerelsesTidspunkt",
                    "dokumenter",
                    "indlaesningsTidspunkt",
                    "regnskab.godkendelse.godkendelsesdato",
                    "regnskab.regnskabsperiode.startDato",
                    "regnskab.regnskabsperiode.slutDato"
                ]
            }

        correlation_id = str(uuid.uuid4())
        context = _new_log_context(correlation_id=correlation_id)
        try:
            response = requests.request(method, url, 
                                        headers=self.headers, 
                                        **kwargs, 
                                        auth=HTTPBasicAuth(self.username, self.password),
                                        json=json
                                        )
        except requests.RequestException as exc:
            LOGGER.log_request_exception(
                "CVR_RequestError",
                correlation_id,
                exc,
                method=method,
                url=url,
                request_headers=self.headers,
                request_body=json,
                context=context,
            )
            raise
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            LOGGER.log_request_failure(
                "CVR_RequestFailed",
                correlation_id,
                response=response,
                method=method,
                url=url,
                request_headers=self.headers,
                request_body=json,
                context=context,
                message=str(exc),
            )
            raise requests.HTTPError(f"{exc}. Response body: {response.text}") from exc
        try:
            payload = response.json()
        except ValueError:
            LOGGER.log_request_failure(
                "CVR_RequestFailed",
                correlation_id,
                response=response,
                method=method,
                url=url,
                request_headers=self.headers,
                request_body=json,
                context=context,
                message="Response body was not valid JSON",
            )
            raise

        LOGGER.log_request_success(
            "CVR_Request",
            correlation_id,
            response=response,
            method=method,
            url=url,
            request_headers=self.headers,
            request_body=json,
            context=context,
        )
        
        return payload, correlation_id
    
    def post(self, body: list, start: str, end: str):
        """
        Execute the standard CVR POST query operation.

        Args:
            body (list): CVR numbers to query.
            start (str): Fiscal period start date (`YYYY-MM-DD`).
            end (str): Fiscal period end date (`YYYY-MM-DD`).

        Returns:
            tuple: `(response_json, transaction_id)`.
        """
        return self._request("POST", body=body, start=start, end=end)
    
    def stable_hash(self, value: str) -> int:
        """
        Compute a stable numeric hash for a string value.

        Args:
            value (str): Input string.

        Returns:
            int: Deterministic 12-digit hash value.
        """
        return int(hashlib.sha256(value.encode()).hexdigest(), 16) % (10**12)

    def _coerce_year(self, year, context: str = "CVR period calculation") -> int:
        """
        Convert a year value to integer, with a clear error for invalid inputs.

        Args:
            year: Year value from user/request/model.
            context: Optional context used in error messages.

        Returns:
            int: Normalized year.

        Raises:
            ValueError: If the year cannot be converted to int.
        """
        try:
            return int(year)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{context}: invalid year value {year!r}") from exc

    def normalize_fiscal_year_end(self, fiscal_raw):
        """
        Normalize a fiscal year-end value into `DDMM` format.

        Args:
            fiscal_raw: Raw fiscal value from source data.

        Returns:
            str | None: Normalized `DDMM` string when possible, else `None`.
        """
        if not fiscal_raw:
            return None
        fiscal = str(fiscal_raw).strip()
        if len(fiscal) == 4 and fiscal.isdigit():
            return fiscal
        try:
            date_part = fiscal.split("T")[0]
            _, m, d = date_part.split("-")
            return f"{int(d):02d}{int(m):02d}"
        except Exception:
            digits = "".join(ch for ch in fiscal if ch.isdigit())
            if len(digits) >= 4:
                return digits[-4:]
            return None

    def calculate_period(self, fiscal_year_end: str, year: int):
        """
        Calculate fiscal period start and end dates for a given year.

        Args:
            fiscal_year_end (str): Fiscal year-end in `DDMM` format.
            year (int): Target year used to resolve period boundaries.

        Returns:
            tuple: `(start_date, end_date)` in `YYYY-MM-DD` format.
        """
        year = self._coerce_year(year, context="calculate_period")
        day = int(fiscal_year_end[:2])
        month = int(fiscal_year_end[2:])

        # Determine the end date of the fiscal year for the given year
        end_date = datetime(year, month, day, tzinfo=timezone.utc)

        # Determine start date
        if month == 12 and day == 31:
            # Calendar year
            start_date = datetime(year, 1, 1, tzinfo=timezone.utc)
        else:
            # Fiscal year spanning two calendar years
            start_month = month + 1
            start_year = year - 1 if start_month <= 12 else year
            start_month = start_month if start_month <= 12 else 1
            start_date = datetime(start_year, start_month, 1, tzinfo=timezone.utc)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def calculate_period_last_year(self, fiscal_year_end: str):
        """
        Calculate the most recently completed fiscal period for a year-end pattern.

        Args:
            fiscal_year_end (str): Fiscal year-end in `DDMM` format.

        Returns:
            tuple: `(start_date, end_date)` in `YYYY-MM-DD` format.
        """
        day = int(fiscal_year_end[:2])
        month = int(fiscal_year_end[2:])

        today = datetime.now(timezone.utc)

        # Determine the end date of the previous fiscal year (the fiscal year that ended before today)
        end_year = today.year
        if (today.month, today.day) <= (month, day):
            end_year -= 1  # the fiscal year hasn't ended yet this year

        end_date = datetime(end_year, month, day, tzinfo=timezone.utc)

        # Determine start date
        if month == 12 and day == 31:
            # Calendar year
            start_date = datetime(end_year, 1, 1, tzinfo=timezone.utc)
        else:
            # Fiscal year spanning two calendar years
            start_month = month + 1
            start_year = end_year - 1 if start_month <= 12 else end_year
            start_month = start_month if start_month <= 12 else 1
            start_date = datetime(start_year, start_month, 1, tzinfo=timezone.utc)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def save_cvr_response(
        self,
        data: dict,
        trans_id: str,
        type: str = "CVR",
        should_cancel: Optional[Callable[[], bool]] = None,
    ):
        """
        Persist CVR API response payload into CVR database tables.

        Args:
            data (dict): CVR API response payload.
            trans_id (str): Correlation id for transaction logging.
            should_cancel (Optional[Callable[[], bool]], optional): Callback that returns True when cancellation is requested.

        Returns:
            None
        """
        db = SQL()
        transaction_row_id = LOGGER.log_transaction(
            f"CVR_{type}",
            trans_id,
            code="200",
            context=_new_log_context(correlation_id=trans_id),
        )

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            return {"transaction_id": transaction_row_id, "records_processed": 0}

        processed = 0
        for hit in hits:
            if should_cancel and should_cancel():
                return {
                    "transaction_id": transaction_row_id,
                    "records_processed": processed,
                }
            source = hit["_source"]
            dokument_ids = []

            # Insert each dokument for this hit
            for doc in source.get("dokumenter", []):
                mime = doc.get("dokumentMimeType") or ""
                if "/" in mime:    
                    suffix = mime.split("/", 1)[1]
                else:
                    suffix = "unknown"
                dokument_row = {
                    "Id": hit["_id"] + "|" + suffix,
                    "DokumentType": doc.get("dokumentType"),
                    "DokumentMimeType": doc.get("dokumentMimeType"),
                    "DokumentUrl": doc.get("dokumentUrl")
                }
                # Insert dokument and get its Id
                ids = db.batch_upsert("cvr.RegnskabsDokument", [dokument_row], pk="Id", return_ids=True)
                if ids:
                    dokument_ids.append(ids[0])
            
            cvr = str(source.get("cvrNummer"))
            start = source.get("regnskab", {}).get("regnskabsperiode", {}).get("startDato")
            end = source.get("regnskab", {}).get("regnskabsperiode", {}).get("slutDato")
            _id = hit["_id"]

            indberetning_row = {
                "Id": _id,
                "Customer_Id": cvr,
                "Transaktion_Id": trans_id
            }
            
            db.batch_upsert(
                "cvr.Indberetninger",
                [indberetning_row],
                pk="Id",
                return_ids=False
            )


            # Insert regnskab
            regnskab_row = {
                "Indberetninger_Id": _id,
                "RegnskabsperiodeStart": start,
                "RegnskabsperiodeSlut": end,
                "Godkendelsesdato": source.get("regnskab", {}).get("godkendelse", {}).get("godkendelsesdato"),
                "OffentliggoerelsesTidspunkt": source.get("offentliggoerelsesTidspunkt"),
                "IndlaesningsTidspunkt": source.get("indlaesningsTidspunkt"),
                "RegnskabsDokument_Id": dokument_ids[0] if dokument_ids else None
            }
            db.batch_upsert("cvr.Regnskab", [regnskab_row], pk="Indberetninger_Id")
            processed += 1

        return {
            "transaction_id": transaction_row_id,
            "records_processed": processed,
        }

    def fetch_all_records(year: int, should_cancel: Optional[Callable[[], bool]] = None):
        """
        Fetch and persist CVR records for all customers in a given year.

        Args:
            year (int): Target fiscal year for period calculation.
            should_cancel (Optional[Callable[[], bool]]): Callback that returns True when cancellation is requested.

        Returns:
            None
        """
        cvr = CVRAPI(api_key=os.getenv('CVR_API_KEY'))
        target_year = cvr._coerce_year(year, context="fetch_all_records")
        run_context = _new_log_context(robot_id="cvr_all")
        run = LOGGER.create_run(
            robot_id="cvr_all",
            trigger_source="cli",
            input_params={"year": target_year},
            context=run_context,
        )
        run.started()

        db = SQL()
        # Example: group customers by FiscalYearEnd so API requests only contain CVRs
        # that share the same fiscal year end. We select FiscalYearEnd first so the
        # underlying paginator orders by it (required by OFFSET/FETCH semantics).
        data_iter = db.get_data(
            "tv.Customer",
            selects=["FiscalYearEnd", "CVR"],
            batch_size=50
        )

        # Group contiguous rows by FiscalYearEnd (db.get_data orders by FiscalYearEnd)
        def fiscal_to_ddmm(fiscal_raw):
            """
            Normalize a fiscal year-end value into `DDMM`.

            Args:
                fiscal_raw: Raw fiscal value from customer data.

            Returns:
                str | None: Normalized `DDMM` value where possible.
            """
            if not fiscal_raw:
                return None
            fiscal = str(fiscal_raw).strip()
            # Already DDMM
            if len(fiscal) == 4 and fiscal.isdigit():
                return fiscal
            # Try ISO date YYYY-MM-DD or datetime
            try:
                date_part = fiscal.split("T")[0]
                y, m, d = date_part.split("-")
                return f"{int(d):02d}{int(m):02d}"
            except Exception:
                # Fallback: remove non-digits and try to pick last 4 digits
                digits = "".join(ch for ch in fiscal if ch.isdigit())
                if len(digits) >= 4:
                    return digits[-4:]
                return fiscal

        def grouped_by_fiscal(rows_iterator, max_per_group: int = 3000):
            """
            Group customer CVRs by contiguous fiscal year-end values.

            Args:
                rows_iterator: Batched row iterator from `SQL.get_data`.
                max_per_group (int): Maximum CVRs emitted per yielded group.

            Yields:
                tuple: `(fiscal_ddmm, cvr_list)` group payload.
            """
            current_fiscal = None
            current_cvrs = []
            for batch in rows_iterator:
                if should_cancel and should_cancel():
                    return
                for row in batch:
                    if should_cancel and should_cancel():
                        return
                    # row[0] == FiscalYearEnd, row[1] == CVR
                    raw_fiscal = row[0]
                    fiscal = fiscal_to_ddmm(raw_fiscal)
                    cvrnum = str(row[1])
                    if fiscal != current_fiscal:
                        if current_cvrs:
                            yield current_fiscal, current_cvrs
                        current_fiscal = fiscal
                        current_cvrs = [cvrnum]
                    else:
                        current_cvrs.append(cvrnum)

                    # If we reach max_per_group, yield and start new
                    if len(current_cvrs) >= max_per_group:
                        yield current_fiscal, current_cvrs
                        current_cvrs = []

            # Yield any remaining
            if current_cvrs:
                yield current_fiscal, current_cvrs

        total_records = 0
        latest_transaction_id = None
        latest_correlation_id = run_context.correlation_id

        for fiscal, cvrs in grouped_by_fiscal(data_iter, max_per_group=50):
            if should_cancel and should_cancel():
                run.cancelled("Cancelled during CVR full sync")
                return

            start, end = cvr.calculate_period(fiscal_year_end=fiscal, year=target_year)
            
            for batch in [cvrs[i:i + 50] for i in range(0, len(cvrs), 50)]:
                if should_cancel and should_cancel():
                    run.cancelled("Cancelled during CVR full sync")
                    return

                body = [str(item) for item in batch]  # row indices must be integers, not str
                transaction_id = run_context.correlation_id
                try:
                    data, transaction_id = cvr.post(body=body, start=start, end=end)
                    if should_cancel and should_cancel():
                        run.cancelled("Cancelled during CVR full sync")
                        return
                    save_result = cvr.save_cvr_response(data, transaction_id, should_cancel=should_cancel)
                    latest_transaction_id = save_result["transaction_id"]
                    latest_correlation_id = transaction_id
                    total_records += save_result["records_processed"]
                    run.attach_transaction(
                        correlation_id=transaction_id,
                        latest_transaction_id=latest_transaction_id,
                    )
                    run.progress(
                        records_processed=total_records,
                        message=f"Persisted {total_records} CVR records",
                    )
                except Exception as exc:
                    LOGGER.log_transaction(
                        "CVR_All_Failed",
                        transaction_id,
                        code=_resolve_log_code(exc),
                        message=str(exc),
                        context=run_context,
                    )
                    run.failed(
                        error_message=str(exc),
                        failure_category="http" if isinstance(exc, requests.RequestException) else "processing",
                        correlation_id=transaction_id,
                    )
                    raise

        run.completed(
            correlation_id=latest_correlation_id,
            latest_transaction_id=latest_transaction_id,
            records_processed=total_records,
            records_succeeded=total_records,
            records_failed=0,
        )

    def fetch_records_by_fiscal_year_end(
        fiscal_year_end: str,
        year: int,
        should_cancel: Optional[Callable[[], bool]] = None,
    ):
        """
        Fetch and persist CVR records for customers with a specific fiscal year-end.

        Args:
            fiscal_year_end (str): Fiscal year-end value used in customer filtering.
            year (int): Target fiscal year for period calculation.
            should_cancel (Optional[Callable[[], bool]]): Callback that returns True when cancellation is requested.

        Returns:
            None
        """
        cvr = CVRAPI(api_key=os.getenv('CVR_API_KEY'))

        db = SQL()
        target_year = cvr._coerce_year(year, context="fetch_records_by_fiscal_year_end")
        run_context = _new_log_context(robot_id="cvr_fiscal")
        run = LOGGER.create_run(
            robot_id="cvr_fiscal",
            trigger_source="cli",
            input_params={"year": target_year, "fiscal_year_end": fiscal_year_end},
            context=run_context,
        )
        run.started()
        data_iter = db.get_data(
            "tv.Customer",
            selects=["CVR"],
            where_clause=f"FiscalYearEnd = '{fiscal_year_end}'",
            batch_size=50
        )

        start, end = cvr.calculate_period(fiscal_year_end=fiscal_year_end, year=target_year)

        cvrs: list[str] = []
        for batch in data_iter:
            if should_cancel and should_cancel():
                run.cancelled("Cancelled during fiscal-year CVR sync")
                return
            for row in batch:
                cvrs.append(str(row[0]))

        total_records = 0
        latest_transaction_id = None
        latest_correlation_id = run_context.correlation_id
        for batch in [cvrs[i:i + 50] for i in range(0, len(cvrs), 50)]:
            if should_cancel and should_cancel():
                run.cancelled("Cancelled during fiscal-year CVR sync")
                return

            body = [str(item) for item in batch]
            transaction_id = run_context.correlation_id
            try:
                data, transaction_id = cvr.post(body=body, start=start, end=end)
                if should_cancel and should_cancel():
                    run.cancelled("Cancelled during fiscal-year CVR sync")
                    return
                save_result = cvr.save_cvr_response(data, transaction_id, should_cancel=should_cancel)
                latest_transaction_id = save_result["transaction_id"]
                latest_correlation_id = transaction_id
                total_records += save_result["records_processed"]
                run.attach_transaction(
                    correlation_id=transaction_id,
                    latest_transaction_id=latest_transaction_id,
                )
                run.progress(
                    records_processed=total_records,
                    message=f"Persisted {total_records} CVR records",
                )
            except Exception as exc:
                LOGGER.log_transaction(
                    f"CVR_FiscalYearEnd_{fiscal_year_end}_Failed",
                    transaction_id,
                    code=_resolve_log_code(exc),
                    message=str(exc),
                    context=run_context,
                )
                run.failed(
                    error_message=str(exc),
                    failure_category="http" if isinstance(exc, requests.RequestException) else "processing",
                    correlation_id=transaction_id,
                )
                raise

        run.completed(
            correlation_id=latest_correlation_id,
            latest_transaction_id=latest_transaction_id,
            records_processed=total_records,
            records_succeeded=total_records,
            records_failed=0,
        )

    def fetch_records_by_cvr(
        cvr_number: str,
        year: int,
        should_cancel: Optional[Callable[[], bool]] = None,
    ):
        """
        Fetch and persist CVR records for a single CVR number.

        Args:
            cvr_number (str): CVR number to query.
            year (int): Target fiscal year for period calculation.
            should_cancel (Optional[Callable[[], bool]]): Callback that returns True when cancellation is requested.

        Returns:
            None
        """
        if should_cancel and should_cancel():
            return

        cvr = CVRAPI(api_key=os.getenv('CVR_API_KEY'))

        db = SQL()
        target_year = cvr._coerce_year(year, context="fetch_records_by_cvr")
        run_context = _new_log_context(robot_id="cvr_single")
        run = LOGGER.create_run(
            robot_id="cvr_single",
            trigger_source="cli",
            input_params={"year": target_year, "cvr": cvr_number},
            context=run_context,
        )
        run.started()
        data_iter = db.get_data(
            "tv.Customer",
            selects=["FiscalYearEnd"],
            where_clause=f"CVR = '{cvr_number}'",
            batch_size=1
        )

        fiscal_year_end = None
        for batch in data_iter:
            if should_cancel and should_cancel():
                run.cancelled("Cancelled during single CVR sync")
                return
            if batch:
                fiscal_year_end = cvr.normalize_fiscal_year_end(batch[0][0])
                break

        # Fallback to calendar year if fiscal year end is missing.
        if not fiscal_year_end:
            fiscal_year_end = "3112"

        start, end = cvr.calculate_period(fiscal_year_end=fiscal_year_end, year=target_year)

        transaction_id = run_context.correlation_id
        try:
            data, transaction_id = cvr.post(body=[cvr_number], start=start, end=end)
            if should_cancel and should_cancel():
                run.cancelled("Cancelled during single CVR sync")
                return
            save_result = cvr.save_cvr_response(
                data,
                transaction_id,
                type="Single",
                should_cancel=should_cancel,
            )
            run.attach_transaction(
                correlation_id=transaction_id,
                latest_transaction_id=save_result["transaction_id"],
            )
            run.completed(
                correlation_id=transaction_id,
                latest_transaction_id=save_result["transaction_id"],
                records_processed=save_result["records_processed"],
                records_succeeded=save_result["records_processed"],
                records_failed=0,
            )
        except Exception as exc:
            LOGGER.log_transaction(
                f"CVR_Single_{cvr_number}_Failed",
                transaction_id,
                code=_resolve_log_code(exc),
                message=str(exc),
                context=run_context,
            )
            run.failed(
                error_message=str(exc),
                failure_category="http" if isinstance(exc, requests.RequestException) else "processing",
                correlation_id=transaction_id,
            )
            raise
