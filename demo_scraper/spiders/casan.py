import json
import os
import re
import sys
import uuid
from datetime import datetime as dt

import pdfkit
import scrapy
from scrapy import FormRequest, Request
from scrapy_splash import SplashRequest

from demo_scraper.config import sample_json

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)


class CasanSpider(scrapy.Spider):
    name = 'casan'
    allowed_domains = ['copasa.com.br']
    result = {}
    matricula = sample_json.get('matricula')
    senha = sample_json.get('senha')
    start_date = sample_json.get('start_date')
    end_date = sample_json.get('end_date')
    start_url = 'http://site.sanepar.com.br/servicos/pagamentos-efetuados'
    scrape_id = sample_json.get('scrape_id')
    result_received = {}
    files_count = 0
    screenshots_count = 0
    splash_args = {
        'html': 1,
        'png': 1,
        'render_all': 1,
        'wait': 0.5
    }
    strip_extra = re.compile(r'\s+')

    def start_requests(self):
        """
        This is the start request module spider calls first this method.
        :return:
        """
        print("The module PATH is", os.path.dirname(__file__))
        yield SplashRequest(self.start_url, callback=self.login_me,
                            endpoint='render.json', args=self.splash_args, dont_filter=True)

    def login_me(self, response):
        """
        This method is used to login to website with credentials
        :param response:
        :return:
        """

        frm_data = {
            'tipo': 'matricula',
            'usuario': self.matricula,
            'senha': self.senha,
            'urlorigem': '',
            'X-Requested-With': 'XMLHttpRequest',
        }

        login_url = "http://atvn.sanepar.com.br/login"
        yield FormRequest(
            url=login_url,
            formdata=frm_data,
            callback=self.redirect_me,
            dont_filter=True
        )

    def redirect_me(self, response):
        """
        This method is used to redirect to main data page.
        :param response:
        :return:
        """

        if 'inválida' in response.text:
            error_msg = {"error_type": "WRONG_CREDENTIALS", "details": "Login ou Senha inválida"}
            return

        cookies = ''
        for cookie in response.headers.getlist('Set-Cookie'):
            if cookies:
                cookies = cookies + '; ' + cookie.decode('utf-8').split(';')[0]
            else:
                cookies = cookie.decode('utf-8').split(';')[0]
        headers = {
            'Cookie': cookies,
            'Referer': 'http://site.sanepar.com.br/servicos/pagamentos-efetuados'
        }

        yield Request(url='http://atvn.sanepar.com.br/login/', callback=self.pagamentos, meta={'Cookie': cookies},
                      headers=headers, dont_filter=True)

    def pagamentos(self, response):
        """
        This method is used to scrape data from main page
        :param response:
        :return:
        """

        Cookie = response.meta['Cookie']

        headers = {
            'Cookie': Cookie,
            'Referer': 'http://site.sanepar.com.br/servicos/debitos-pendentes'
        }

        self.result['matricula'] = self.strip_extra.sub(" ", str(
            response.selector.xpath('//div[text()="Matricula:"]/span/text()').get(""))).strip()

        pagamentos_efetuados = []
        pagamentos_efetuados_list = response.selector.xpath('//*[@id="gridContent"]//tbody/tr')
        for pagamentos in pagamentos_efetuados_list:
            if self.strip_extra.sub(" ", str(pagamentos.xpath('./td[1]/text()').get(""))).strip():
                referencia = self.strip_extra.sub(" ", str(pagamentos.xpath('./td[1]/text()').get(""))).strip()
                vencimento = self.strip_extra.sub(" ", str(pagamentos.xpath('./td[2]/text()').get(""))).strip()
                pagamento = self.strip_extra.sub(" ", str(pagamentos.xpath('./td[3]/text()').get(""))).strip()
                vencimento_datetime = dt.strptime(vencimento, "%d/%m/%Y")

                if (not self.start_date and not self.end_date) or (
                        (self.start_date and not self.end_date) and self.start_date <= vencimento_datetime) or (
                        (self.end_date and not self.start_date) and vencimento_datetime <= self.end_date) or \
                        ((
                                 self.end_date and self.start_date) and self.start_date <= vencimento_datetime <= self.end_date):
                    banco = self.strip_extra.sub(" ", str(pagamentos.xpath('./td[4]/text()').get(""))).strip()
                    agencia = self.strip_extra.sub(" ", str(pagamentos.xpath('./td[5]/text()').get(""))).strip()
                    valor = self.strip_extra.sub(" ", str(pagamentos.xpath('./td[6]/text()').get(""))).strip()
                    pagamentos_efetuados.append(
                        {
                            'referencia': referencia,
                            'vencimento': vencimento,
                            'pagamento': pagamento,
                            'banco': banco,
                            'agencia': agencia,
                            'valor': valor,
                        }
                    )

        if pagamentos_efetuados:
            self.result['pagamentos_efetuados'] = pagamentos_efetuados

        yield Request(url='http://atvn.sanepar.com.br/login/', callback=self.debitos, meta={'Cookie': Cookie},
                      headers=headers, dont_filter=True)

    def debitos(self, response):
        """
        This method is used to get other page data
        :param response:
        :return:
        """

        Cookie = response.meta['Cookie']

        status = self.strip_extra.sub(" ",
                                      str(response.selector.xpath('//div[@class="Titulo"]/text()').get(""))).strip()
        if status != 'Debitos Pendentes - Conta':
            self.result['status'] = status

        if 'status' not in self.result:
            debitos_pendentes = []
            debitos_pendentes_list = response.selector.xpath('//*[@id="gridConta"]//tbody/tr')
            for debitos in debitos_pendentes_list:
                if self.strip_extra.sub(" ", str(debitos.xpath('./td[1]/text()').get(""))).strip():
                    referencia = self.strip_extra.sub(" ", str(debitos.xpath('./td[1]/text()').get(""))).strip()
                    vencimento = self.strip_extra.sub(" ", str(debitos.xpath('./td[2]/text()').get(""))).strip()
                    valor = self.strip_extra.sub(" ", str(debitos.xpath('./td[3]/text()').get(""))).strip()
                    multa = self.strip_extra.sub(" ", str(debitos.xpath('./td[4]/text()').get(""))).strip()
                    total = self.strip_extra.sub(" ", str(debitos.xpath('./td[5]/text()').get(""))).strip()
                    vencimento_link = debitos.xpath('./td[6]/a/@href').get("")
                    if vencimento_link:
                        vencimento_link = 'http://atvn.sanepar.com.br' + vencimento_link
                        headers = {
                            'Cookie': Cookie,
                        }

                        debitos_pendentes.append(
                            {
                                'referencia': referencia,
                                'vencimento': vencimento,
                                'valor': valor,
                                'multa': multa,
                                'total': total,
                            }
                        )

                        yield Request(
                            url=vencimento_link,
                            callback=self.save_pdf,
                            headers=headers,
                            meta={
                                "result_key": 'debitos_pendentes',
                                'file_type': 'boleto',
                                'vencimento': vencimento,
                            },
                            dont_filter=True
                        )

            if debitos_pendentes:
                self.result['debitos_pendentes'] = debitos_pendentes

    def save_pdf(self, response):
        """
        This method is used to save the pdf in the site.
        :param response:
        :return:
        """

        # get metadata
        vencimento = response.meta['vencimento']
        result_key = response.meta['result_key']
        file_type = "__{file_type}__".format(
            file_type=response.meta['file_type'])

        # options to save pdf
        file_id = str(uuid.uuid4())
        filename = "{file_id}.pdf".format(file_id=file_id)
        file_path = os.path.join(path, "downloads", filename)
        options = {
            'page-size': 'A4',
            'encoding': "UTF-8",
            'enable-local-file-access': None
        }
        html_text = response.body.decode(
            "iso-8859-1").replace(
            '/Content/', 'http://atvn.sanepar.com.br/Content/'
        )
        pdfkit.from_string(html_text, file_path, options=options)

        # upload pdf to s3 and call the webhook

        # update values in result
        result_value = self.result.get(result_key, [])
        for item in result_value:
            if item['vencimento'] == vencimento:
                item.update({
                    file_type: {
                        "file_id": file_id}
                })
        self.result.update({result_key: result_value})

    def closed(self, spider):
        """
        Will be called before spider closed
        Used to save data_collected result
        :param spider:
        :return:
        """

        # stop crawling after yeild_item called
        if not self.result_received:
            # push to webhook
            data = {'scrape_id': self.scrape_id,
                    'scraper_name': self.name,
                    'files_count': self.files_count,
                    'screenshots_count': self.screenshots_count}
            data.update({'result': self.result})
            webhook_file_path = os.path.join(
                path, "downloads", '{matricula}-data_collected.json'.format(
                    matricula=self.matricula))
            json_file = open(webhook_file_path, 'wb')
            json_file.write(json.dumps(data, indent=4, sort_keys=True, ensure_ascii=False).encode("utf-8"))
