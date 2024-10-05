#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "David Parra <parrade94[at]hotmail.com>"
__version__ = "1.0.0"

import uvicorn as uvicorn
from fastapi import FastAPI, Header, status
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import http.client
import pycurl
from io import BytesIO, StringIO
from bs4 import BeautifulSoup
import json
import pandas as pd


middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*'],
        max_age=3600,
    )
]


app = FastAPI(title='API_WEB-SCRAPING', middleware=middleware)


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)


@app.get("/")
async def root():
    try:
        data = "testOK"
        return JSONResponse(status_code=status.HTTP_200_OK, content={'mensaje': 'success', 'error': False, 'data': data})
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={'mensaje': str(e), 'error': True})


@app.get("/script")
async def script(lang: str = Header(default="es"), usuario: str = Header(default=None), clave: str = Header(default=None)):
    try:
        conn = http.client.HTTPSConnection("storage.googleapis.com")
        payload = ''
        headers = {}
        conn.request("GET", "/resources-prod-shelftia/scrapers-prueba/product.json", payload, headers)
        res = conn.getresponse()
        data = res.read()
        data = data.decode("utf-8")

        objs = {
            "allergens" : "",
            "sku" : "",
            "vegan" : "",
            "kosher" : "",
            "organic" : "",
            "vegetarian" : "",
            "gluten_free" : "",
            "lactose_free" : "",
            "package_quantity" : "",
            "unit_size" : "",
            "net_weight" : ""
        }
        nt = lang + "-CR"
        dcsv = []

        for nodo in json.loads(data)["allVariants"][0]["attributesRaw"]:
            if nodo["name"] in ['custom_attributes']:   
                data = json.loads(nodo["value"][nt])
                objs["allergens"] = [data["allergens"]["value"][0]["name"]]
                objs["sku"] = data["sku"]["value"]
                objs["vegan"] = data["vegan"]["value"]
                objs["kosher"] = data["kosher"]["value"]
                objs["organic"] = data["organic"]["value"]
                objs["vegetarian"] = data["vegetarian"]["value"]
                objs["gluten_free"] = data["gluten_free"]["value"]
                objs["lactose_free"] = data["lactose_free"]["value"]
                objs["package_quantity"] = float(data["package_quantity"]["value"])
                objs["unit_size"] = float(data["unit_size"]["value"])
                objs["net_weight"] = float(data["net_weight"]["value"])

                dcsv.append(objs)
        
        df = pd.DataFrame(dcsv)
        stream = StringIO()
        df.to_csv(stream, index = False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv"
                                    )
        response.headers["Content-Disposition"] = "attachment; filename=output.csv"
        return response
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={'mensaje': str(e), 'error': True})



@app.post("/webscraping")
async def webscraping(url: str, usuario: str = Header(default=None), clave: str = Header(default=None)):
    try:
        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        c.perform()
        c.close()
        body = buffer.getvalue()        
        html = buffer.getvalue().decode("utf-8")
        soup = BeautifulSoup(html, "html.parser")
        title = soup.find("title")
        script = soup.find_all('template', attrs={"data-varname":"__STATE__"})
        data = []
        return JSONResponse(status_code=status.HTTP_200_OK, content=data)
    except Exception as e:
        return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={'mensaje': str(e), 'error': True})


if __name__ == "__main__":
    uvicorn.run('api:app', host='0.0.0.0', port=5053, workers=1, reload=True)