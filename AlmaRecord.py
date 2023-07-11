# -*- coding: utf-8 -*-
import os
# external imports
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import logging
import xml.etree.ElementTree as ET
from math import *
import re


__version__ = '0.1.0'
__apikey__ = os.getenv('ALMA_API_KEY')
__region__ = os.getenv('ALMA_API_REGION')

FORMATS = {
    'json': 'application/json',
    'xml': 'application/xml'
}

# RESOURCES = {
#     'get_holding' : 'bibs/{bib_id}/holdings/{holding_id}',
#     'get_holdings_list' : 'bibs/{bib_id}/holdings',
#     'get_item_with_barcode' : 'items?item_barcode={barcode}',
#     'get_item' : 'bibs/{bib_id}/holdings/{holding_id}/items/{item_id}',
#     'get_set' : 'conf/sets/{set_id}',
#     'get_set_members' : 'conf/sets/{set_id}/members?limit={limit}&offset={offset}',
#     'get_record' : 'bibs/{mms_id}?view={view}&expand={expand}',
#     'retrieve_record' : 'bibs?{id_type}={bib_id}&view={view}&expand={expand}',
#     'update_record' : 'bibs/{bib_id}',
#     'get_item_requests_list' : 'bibs/{bib_id}/holdings/{holding_id}/items/{item_id}/requests?request_type={request_type}&status={status}',
#     'delete_request' : 'bibs/{bib_id}/holdings/{holding_id}/items/{item_id}/requests/{request_id}?reason={reason}&notify_user={notify_user}',
#     'ending_process' : 'bibs/{bib_id}/holdings/{holding_id}/items/{item_id}?op=scan&library={library_code}&department={department}&done=true',
#     'test' : 'bibs/test'
# }

NS = {'sru': 'http://www.loc.gov/zing/srw/',
        'marc': 'http://www.loc.gov/MARC21/slim',
        'xmlb' : 'http://com/exlibris/urm/general/xmlbeans'
         }

class AlmaRecord(object):
    """A set of function for interact with Alma Apis in area "Records & Inventory"
    """

    def __init__(self, mms_id,view='full',expand='None',accept='json', apikey=__apikey__, service='AlmaPy') :
        if apikey is None:
            raise Exception("Merci de fournir une clef d'APi")
        self.apikey = apikey
        self.service = service
        self.error_status = False
        self.logger = logging.getLogger(service)
        self.logger.debug("Youpi")
        status,response = self.request('GET', 
                                       'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/bibs/{}?view={}&expand={}'.format(mms_id,view,expand),
                                        accept=accept)
        if status == 'Error':
            self.error_status = True
            self.error_message = response
        else:
            self.doc = self.extract_content(response)
        

    @property
    #Construit la requête et met en forme les réponses

    def headers(self, accept='json', content_type=None):
        headers = {
            "User-Agent": "pyalma/{}".format(__version__),
            "Authorization": "apikey {}".format(self.apikey),
            "Accept": FORMATS[accept]
        }
        if content_type is not None:
            headers['Content-Type'] = FORMATS[content_type]
        return headers
    def get_error_message(self, response, accept):
        """Extract error code & error message of an API response
        
        Arguments:
            response {object} -- API REsponse
        
        Returns:
            int -- error code
            str -- error message
        """
        error_code, error_message = '',''
        if accept == 'xml':
            root = ET.fromstring(response.text)
            error_message = root.find(".//xmlb:errorMessage",NS).text if root.find(".//xmlb:errorMessage",NS).text else response.text 
            error_code = root.find(".//xmlb:errorCode",NS).text if root.find(".//xmlb:errorCode",NS).text else '???'
        else :
            try :
             content = response.json()
            except : 
                # Parfois l'Api répond avec du xml même si l'en tête demande du Json cas des erreurs de clefs d'API 
                root = ET.fromstring(response.text)
                error_message = root.find(".//xmlb:errorMessage",NS).text if root.find(".//xmlb:errorMessage",NS).text else response.text 
                error_code = root.find(".//xmlb:errorCode",NS).text if root.find(".//xmlb:errorCode",NS).text else '???'
                return error_code, error_message 
            error_message = content['errorList']['error'][0]['errorMessage']
            error_code = content['errorList']['error'][0]['errorCode']
        return error_code, error_message
    
    def request(self, httpmethod, url, params={}, data=None,
                accept='json', content_type=None, nb_tries=0, in_url=None):
        #20190905 retry request 3 time s in case of requests.exceptions.ConnectionError
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        response = session.request(
            method=httpmethod,
            headers={
            "User-Agent": "pyalma/{}".format(__version__),
            "Authorization": "apikey {}".format(self.apikey),
            "Accept": FORMATS[accept]
        },
            url= url,
            params=params,
            data=data)
        try:
            response.raise_for_status()  
        except requests.exceptions.HTTPError:
            if response.status_code == 400 :
                error_code, error_message= self.get_error_message(response,accept)
                self.logger.error("Alma_Apis :: Connection Error: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
                return 'Error', "{} -- {}".format(error_code, error_message)
            else :
                error_code, error_message= self.get_error_message(response,accept)
            if error_code == "401873" :
                return 'Error', "{} -- {}".format(error_code, "Notice innconnue")
            self.logger.error("Alma_Apis :: HTTP Status: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
            return 'Error', "{} -- {}".format(error_code, error_message)
        except requests.exceptions.ConnectionError:
            error_code, error_message= self.get_error_message(response,accept)
            self.logger.error("Alma_Apis :: Connection Error: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
            return 'Error', "{} -- {}".format(error_code, error_message)
        except requests.exceptions.RequestException:
            error_code, error_message= self.get_error_message(response,accept)
            self.logger.error("Alma_Apis :: Connection Error: {} || Method: {} || URL: {} || Response: {}".format(response.status_code,response.request.method, response.url, response.text))
            return 'Error', "{} -- {}".format(error_code, error_message)
        return "Success", response

            

    
    def extract_content(self, response):
        ctype = response.headers['Content-Type']
        if 'json' in ctype:
            return response.json()
        else:
            return response.content.decode('utf-8')

    def is_cz_record(self):
        if not self.doc['linked_record_id'] :
            return False
        else :
            return True

    def is_unimarc_record(self):
        if self.doc['record_format'] == 'unimarc' :
            return True
        else :
            return False

    def is_abes_record(self):
        if self.doc['originating_system'] == 'ABES' or re.search(r"SUDOC",self.doc['originating_system']) :
            return True
        else :
            return False
    
