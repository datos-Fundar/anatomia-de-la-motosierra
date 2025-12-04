import re
import io
import os
import base64
import requests
import traceback
import dateparser
from zoneinfo import ZoneInfo
from datetime import datetime, date
from functools import wraps
from bs4 import BeautifulSoup
from typing import Optional, List
from abc import ABC, abstractmethod
from bs4.element import PageElement, Tag
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from urllib3.exceptions import ProtocolError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import pdfminer.high_level as pdf
from suitable_class_finder import SuitableClassFinder
from chainsaw.model.scrapping import ScrappedInfo


connection_errors = (
    ConnectionAbortedError,
    requests.exceptions.ConnectionError,
    ProtocolError,
    WebDriverException
)


def safe_process(method):
    @wraps(method)
    def wrapper(cls, url, driver):
        try:
            return method(cls, url, driver)
        except Exception as e:
            now = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires"))
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            with open(os.path.join("errors.txt"), "a", encoding="utf-8") as f:
                f.write(f"[{timestamp}] > Error en {cls.__name__}.process('{url}'): {e}\n")
            traceback.print_exc()
            return IgnoreLinkScrapper.process(url, driver)
    return wrapper


class OfficialDocumentScrapper(ABC):
    """
    Abstract base class for scrappers that handle official documents.
    """
    @classmethod
    def working_on(cls, url: str, driver) -> List[ScrappedInfo]:
        scrapper_subclass = SuitableClassFinder(cls).suitable_for(
            url,
            default_subclass=IgnoreLinkScrapper,
        )
        return scrapper_subclass.process(url, driver)

    @classmethod
    @abstractmethod
    def process(cls, url: str, driver) -> List[ScrappedInfo]:
        """
        Process the BeautifulSoup object and return the content of the official document 
        with its url. Because it could be divided on multiple documents as
        other links, pdf files, etc, the final result is a list.
        """
        pass

    @classmethod
    @abstractmethod
    def can_handle(cls, url: str) -> bool:
        """
        Check if the scrapper can handle the given URL.
        """
        pass

    @classmethod
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(connection_errors),
        reraise=True
    )
    def get_soup_of(
        cls,
        url: str,
        driver,
        wait_selector: Optional[str] = None,
        timeout: int = 15,
    ) -> BeautifulSoup:
        """
        Fetch the content of the URL and return a BeautifulSoup object.
        """
        driver.get(url)

        if wait_selector:
            try:
                WebDriverWait(driver, timeout).until(
                    lambda d: d.find_element(By.CSS_SELECTOR, wait_selector).text.strip() != ""
                )
            except TimeoutException:
                print(f"[!] Timeout esperando selector '{wait_selector}' en {url}")
            except Exception as e:
                print(f"[!] Error esperando selector '{wait_selector}' en {url}: {e}")

        html = driver.page_source
        return BeautifulSoup(html, 'html.parser')


class IgnoreLinkScrapper(OfficialDocumentScrapper):
    @classmethod
    def can_handle(cls, url: str) -> bool:
        return False

    @classmethod
    def process(cls, url: str, driver) -> List[ScrappedInfo]:
        return []


class InfolegScrapper(OfficialDocumentScrapper):
    BASE_URL = "https://servicios.infoleg.gob.ar/infolegInternet"
    LINK_TEXT = "Texto completo de la norma"

    @classmethod
    def can_handle(cls, url: str) -> bool:
        """
        Check if the scrapper can handle Infoleg URLs.
        """
        return "infoleg.gob.ar" in url

    @classmethod
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(connection_errors),
        reraise=True
    )
    def __get_date(cls, soup: BeautifulSoup, url: str) -> date:
        p_tags = soup.find_all('p')
        for p in p_tags:
            if "Publicada en el Boletín Oficial del" in p.get_text():
                date_link = p.find('a')
                if date_link:
                    raw_date = date_link.get_text(strip=True)
                    parsed = dateparser.parse(raw_date, languages=['es'])
                    if parsed:
                        return parsed.date()
                    else:
                        print(f"[!] No se pudo parsear la fecha con dateparser: {raw_date}")

        text = soup.get_text()
        match = re.search(
            r'(?:Bs\. ?As\.|Buenos Aires),?\s*[\n\r]*\s*(?:(\d{1,2})/(\d{1,2})/(\d{2,4})|(\d{1,2}) de (\w+) de (\d{2,4}))',
            text,
            flags=re.IGNORECASE,
        )

        if match and match.group(1):
            day, month, year = match.group(1), match.group(2), match.group(3)
            raw_date = f"{day}/{month}/{year}"
            try:
                format = "%d/%m/%y" if len(year) == 2 else "%d/%m/%Y"
                return datetime.strptime(raw_date, format).date()
            except ValueError:
                print(f"[!] No se pudo parsear la fecha en formato Bs.As.: {raw_date}")
        elif match and match.group(4):
            day, month_name, year = match.group(4), match.group(5), match.group(6)
            month_map = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
                'julio': 7, 'agosto': 8, 'septiembre': 9, 'setiembre': 9,
                'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
            month = month_map[month_name.lower()]
            return datetime(int(year), month, int(day)).date()
        raise ValueError(f"No se encontró una fecha válida en el documento {url}")

    @classmethod
    @safe_process
    def process(cls, url: str, driver) -> List[ScrappedInfo]:
        """
        Process the BeautifulSoup object found on Infoleg webservice.
        Then it looks for a link with the text "Texto completo de la norma" and fetches the full document.
        """
        results = []
        soup = cls.get_soup_of(url, driver, wait_selector="body")
        current_date = cls.__get_date(soup, url)
        results.append(
            ScrappedInfo(
                url=url,
                text=soup.text.lower(),
                date=current_date,
            )
        )

        for a_tag in soup.find_all('a'):
            b_tag = a_tag.find('b')
            if b_tag and b_tag.get_text(strip=True) == cls.LINK_TEXT:
                full_norm_url = f"{cls.BASE_URL}/{a_tag['href']}"
                full_norm_soup = cls.get_soup_of(
                    full_norm_url,
                    driver,
                    wait_selector="body",
                )
                results.append(
                    ScrappedInfo(
                        url=full_norm_url,
                        text=full_norm_soup.text.lower(),
                        date=current_date,
                    )
                )
        return results


class BoletinOficialScrapper(OfficialDocumentScrapper):
    BASE_URL = "https://www.boletinoficial.gob.ar"

    @classmethod
    def can_handle(cls, url: str) -> bool:
        """
        Check if the scrapper can handle Boletin Oficial URLs.
        """
        return any((host in url for host in ("boletinoficial.gob.ar", "boletinoficial.gov.ar")))

    @classmethod
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(connection_errors),
        reraise=True
    )
    def __get_date(cls, soup: BeautifulSoup, url: str) -> date:
        p_tag = soup.find('p', class_='text-muted')
        if p_tag:
            text = p_tag.get_text(strip=True)
            match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", text)
            if match:
                raw_date = match.group(1)
                try:
                    return datetime.strptime(raw_date, "%d/%m/%Y").date()
                except ValueError:
                    print(f"[!] No se pudo parsear la fecha: {raw_date}")
        raise ValueError(f"No se encontró una fecha válida en el documento {url}")

    @classmethod
    def __is_there_content(cls, soup: BeautifulSoup) -> Optional[PageElement]:
        """
        Check if the soup contains the content of the official document.
        """
        return soup.find("div", class_="avisoContenido")

    @classmethod
    def __is_there_embedded_pdf(cls, tag: Tag) -> Optional[str]:
        """
        Check if the tag contains a base64 encoded PDF.
        """
        base64_pdf = None
        if tag.string and "convertBase64InUrlBlob" in tag.string:
            match = re.search(
                r'convertBase64InUrlBlob\("([^"]+)"\)',
                tag.string,
            )
            if match:
                base64_pdf = match.group(1)
        return base64_pdf

    @classmethod
    def __are_there_attachments(cls, soup: BeautifulSoup) -> Optional[PageElement]:
        """
        Check if the soup contains any attachments.
        """
        return soup.find("div", id="anexosDiv")

    @classmethod
    def __get_attachments_from(
        cls,
        attachments_div: PageElement,
        source_url: str,
        current_date: date,
    ) -> List[ScrappedInfo]:
        """
        Extract the URLs of the attachments from the soup.
        """
        attachments_list = []
        attachments = attachments_div.find_all("div", class_="panel-body")
        if not attachments:
            return attachments_list

        for panel in attachments:
            onclick = panel.get("onclick", "")
            match = re.search(
                r'descargarPDFAnexo\("([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)",\s*"([^"]+)"\)',
                onclick,
            )

            if not match:
                print(f"BoletinOficialScrapper: it was not possible to find a match {onclick}")
                continue

            section, attachment_number, attachment_id, publish_date, url_pdf = match.groups()
            url = f"{cls.BASE_URL}/{url_pdf}"

            payload = {
                "seccion": section,
                "nroAnexo": attachment_number,
                "idAnexo": attachment_id,
                "fechaPublicacion": publish_date
            }

            headers = {
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest"
            }

            response = requests.post(url, data=payload, headers=headers)
            if not response.ok:
                print(f"BoletinOficialScrapper: Error downloading attachment {attachment_number} from {source_url}: status {response.status_code}")
                continue

            json_data = response.json()
            pdf_base64 = json_data.get("pdfBase64")
            if not pdf_base64:
                print(f"BoletinOficialScrapper: base64 not found for {attachment_number} from {source_url}: status {response.status_code}")
                continue

            pdf_bytes = base64.b64decode(pdf_base64)
            pdf_file = io.BytesIO(pdf_bytes)
            attachments_list.append(
                ScrappedInfo(
                    url=f"Anexo {attachment_number}: {source_url}",
                    text=pdf.extract_text(pdf_file).lower(),
                    date=current_date,
                )
            )
        return attachments_list

    @classmethod
    @safe_process
    def process(cls, url: str, driver) -> List[ScrappedInfo]:
        """
        Process the BeautifulSoup object found on Boletin Oficial web.
        """
        results = []
        soup = cls.get_soup_of(url, driver, wait_selector="p.text-muted")
        current_date = cls.__get_date(soup, url)
        if (content := cls.__is_there_content(soup)):
            paragraphs = content.get_text(separator="\n").strip()
            results.append(
                ScrappedInfo(
                    url=url,
                    text=paragraphs.lower(),
                    date=current_date,
                )
            )

        for script_tag in soup.find_all("script"):
            if (base64_pdf := cls.__is_there_embedded_pdf(script_tag)):
                pdf_bytes = base64.b64decode(base64_pdf)
                pdf_file = io.BytesIO(pdf_bytes)
                results.append(
                    ScrappedInfo(
                        url=url,
                        text=pdf.extract_text(pdf_file).lower(),
                        date=current_date,
                    )
                )

        if (attachments_div := cls.__are_there_attachments(soup)):
            results.extend(
                cls.__get_attachments_from(
                    attachments_div,
                    url,
                    current_date,
                )
            )
        return results
