

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
from datetime import datetime, timezone, timedelta
from API.http_logging import (
    extract_error_code,
    read_float_env,
    read_int_env,
)

dotenv.load_dotenv()


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

        def get_headers(trans_id):
            return {
                "Authorization": f"Bearer {self.auth.access_token}",
                "Content-Type": "application/json",
                "X-Transaktions-ID": trans_id,
                "X-Request-ID": str(uuid4()),
                "UFST-Adgangsgrundlag": os.getenv("DUPLA_ADGANGSGRUNDLAG")
            }
        
        all_data = []
        next_url = url
        params = {
            "VirksomhedSENummer": cvr,
            "SelskabSelvangivelseIndkomstÅr": year,
        }

        while next_url:
            if should_cancel and should_cancel():
                break
            # Generate new transaction ID for each request
            current_trans_id = str(uuid4())
            headers = get_headers(current_trans_id)
            
            if next_url == url:
                # First request - use params
                try:
                    response = requests.get(next_url,
                                           headers=headers,
                                           cert=self.auth.cert,
                                           params=params,
                                           verify=True
                                           )
                except requests.RequestException as exc:
                    error_code = extract_error_code(getattr(exc, "response", None))
                    self.sql.log(
                        f'DUPLA_Selvangivelse_RequestError_{cvr}',
                        current_trans_id,
                        code=error_code
                    )
                    raise
            else:
                # Subsequent requests - use full URL from next link
                try:
                    response = requests.get(next_url,
                                           headers=headers,
                                           cert=self.auth.cert,
                                           verify=True
                                           )
                except requests.RequestException as exc:
                    error_code = extract_error_code(getattr(exc, "response", None))
                    self.sql.log(
                        f'DUPLA_Selvangivelse_RequestError_{cvr}',
                        current_trans_id,
                        code=error_code
                    )
                    raise
 
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                error_code = extract_error_code(response)

                self.sql.log(
                    f'DUPLA_Selvangivelse_Failed_{cvr}',
                    current_trans_id,
                    code=error_code
                )
                raise requests.HTTPError(f"{exc}. Response body: {response.text}") from exc
            page_data = response.json()
            
            all_data.extend(page_data.get('data', []))
            
            next_url = page_data.get('links', {}).get('next')
            if next_url:
                time.sleep(_rate_limit_delay())  # rate limiting
            
        return {'data': all_data}, trans_id
    
    def save_selskabselvangivelse_response(
            self,
            payload: dict,
            correlation_id: str,
            should_cancel: Optional[Callable[[], bool]] = None,
    ):
        """
        Persists DUPLA Selskabsselvangivelser payload to normalized schema table

        Args:
            payload (dict): DUPLA API response payload containing selskabsselvangivelser data.
            correlation_id (str): Correlation id used for transaction logging.
            should_cancel (Optional[Callable[[], bool]], optional): Callback that returns True when cancellation is requested. Defaults to None.
        """
        if 'data' not in payload:
            raise ValueError("Invalid payload: 'data' key not found")

        # ---- LOG TRANSAKTION ---- #
        transaktions_id = self.sql.log(

            'DUPLA_Selvangivelse',
            correlation_id,
            code="200"
        )
    
        for item in payload['data']:
            if should_cancel and should_cancel():
                break
            attr = item.get('attributes', {})
            selvangivelseOplysninger = attr.get('SelvangivelseOplysninger', {})
            teknisk_registrering_dato = selvangivelseOplysninger.get('TekniskRegistreringDato') or ""
            teknisk_registrering_aar = (
                str(teknisk_registrering_dato).split("-", 1)[0]
                if teknisk_registrering_dato
                else ""
            )

            selvangivelse_pk = f"{self.onbehalfofse}_{teknisk_registrering_aar}"
            selvangivelse = {
                "id": selvangivelse_pk,
                "CustomerId": self.onbehalfofse,
                "TekniskRegistreringDato": selvangivelseOplysninger.get('TekniskRegistreringDato'),
                "IndkomstÅr": selvangivelseOplysninger.get('SelskabSelvangivelseIndkomstÅr'),
                "DelperiodeStart": selvangivelseOplysninger.get('SelvangivelseDelperiodeStart'),
                "DelperiodeSlut": selvangivelseOplysninger.get('SelvangivelseDelperiodeSlut'),
                "OpdateretTimestamp" : selvangivelseOplysninger.get('SelskabSelvangivelseOpdateretTimestamp')
            }

            self.sql.batch_upsert('selskabsskat.Selvangivelse', [selvangivelse], pk=['id'], return_ids = False)
            
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
        max_workers = read_int_env("SELSKABSSKATTEOPLYSNINGER_MAX_WORKERS", 10, min_value=1)
        semaphore = threading.BoundedSemaphore(value=max_workers)

        sql = SQL()
        batches = sql.get_data(
            "tv.Customer",
            selects=["CVR"],
            where_clause=f"LEN(CVR) >= 8",
            batch_size=25
        )
        
        cvrs: list[str] = []
        for batch in batches:
            if should_cancel and should_cancel():
                return
            for (cvr_value,) in batch:
                cvr_str = str(cvr_value)
                if len(cvr_str) >= 8:
                    cvrs.append(cvr_str)

        def process_one_cvr(cvr_value: str) -> None:
            """
                Process one CVR number for selskabsselvangivelse.

                Args:
                    cvr_value (str): 8-digit CVR number.

                Returns:
                None
            """
            corr_id = str(uuid4())
            if should_cancel and should_cancel():
                return
            with semaphore:
                try:
                    worker_sql = SQL()
                    auth = Dupla(onbehalfofse=cvr_value)
                    service = SelskabsskatteoplysningerService(auth, worker_sql)

                    payload, corr_id = service.fetch_selskabselvangivelse(year, cvr_value, should_cancel=should_cancel)
                    if should_cancel and should_cancel():
                        return
                    service.save_selskabselvangivelse_response(payload, corr_id, should_cancel=should_cancel)

                    time.sleep(_rate_limit_delay())  # rate limiting

                except Exception as e:
                    if isinstance(e, requests.HTTPError):
                        error_code = extract_error_code(e.response)
                    else:
                        error_code = 400
                    sql.log(
                        f'DUPLA_Selvangivelse_Failed_{cvr_value}',
                        corr_id,
                        code=error_code
                    )
                    time.sleep(_rate_limit_delay())  # rate limiting

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for cvr_value in cvrs:
                if should_cancel and should_cancel():
                    break
                futures.append(pool.submit(process_one_cvr, cvr_value))

            for future in as_completed(futures):
                if should_cancel and should_cancel():
                    for pending in futures:
                        pending.cancel()
                    break
                # surface unexpected thread exceptions while keeping the batch running
                future.result()

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

        if len(cvr) >= 8:
            corr_id = str(uuid4())
            try:
                auth = Dupla(onbehalfofse=str(cvr))
                service = SelskabsskatteoplysningerService(auth, sql)

                payload, corr_id = service.fetch_selskabselvangivelse(year, cvr, should_cancel=should_cancel)
                if should_cancel and should_cancel():
                    return
                service.save_selskabselvangivelse_response(payload, corr_id, should_cancel=should_cancel)

            except Exception as e:
                if isinstance(e, requests.HTTPError):
                    error_code = extract_error_code(e.response)
                else:
                    error_code = 400
                sql.log(
                    f'DUPLA_Selvangivelse_Failed_{cvr}',
                    corr_id,
                    code=error_code
                )
