# Importamos las librerias necesarias:

import requests
import numpy as np
from bs4 import BeautifulSoup
import pandas
import os
import shutil


def create_dic(array):
    """
    Esta función toma un array y extrae los nombres de las columnas de la
    primera fila y el resto de filas para los registros.

    Atributos
    ----------
    array -> array con la primera fila el nombre de las columnas y 
             el resto los registros.
    """

    s = np.shape(array)
    n_col = s[1]
    col = array[0, :]

    d = dict()

    for i in range(n_col):
        d.update({col[i]: list(array[1:, i])})

    return d


"""
1. TOMA DE DATOS DE LA PÁGINA PRINCIPAL

En esta sección se trata en hacer web scraping sobre la página principal de la
bolsa de madrid y a partir de ahí se estraeran los datos para el resto de las
extracción de los datos.
"""
#  Elegimos la web para realizar web scraping

web = "https://www.bolsamadrid.es"

#  Ahora preparamos la cabecera para la request

headers = {
'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) '
'AppleWebKit/537.36 (KHTML, like Gecko) '
'Chrome/83.0.4103.97 Safari/537.36'}

#  Realizamos la petición

page = requests.get(web, headers=headers)

print("Código del estado de la respuestas: {0}".format(page.status_code))

# Utilizamos la libreria BeautifulSoup para trabajar con el contenido de la web

soup = BeautifulSoup(page.content)

# Tabla de los datos diarios
tabla = soup.find('table', attrs={"class":"TblPort TblAccPort"})

# Se extren las urls de la tabla para poder aceder a todos los activos.
urls = []
for fila in tabla.find_all("a"):
    a = fila.get("href")
    b = fila.get_text()
    line = [b, a]
    urls.append(line)

"""
2. EXTRACCIÓN DE LOS DATOS A PARTIR DE LAS URLS DE LA PÁGINA PRINCIPAL
"""
# Establecemos los nombres de las columnas

m3 = [["Id", 'Fecha', 'Cierre', 'Referencia', 'Volumen', 'Efectivo', 'Último',
       'Máximo', 'Mínimo', 'Medio', 'Activo']]
# Realizamos web scraping en cada una de las urls almacenadas en el paso previo
k = 0
for i in range(len(urls)):
    ext = urls[i][1].split("?")
    activo = urls[i][0]
    webi = str(web) + "/esp/aspx/Empresas/InfHistorica.aspx?" + str(ext[1])
    pagei = requests.get(webi, headers=headers)
    soupi = BeautifulSoup(pagei.content)
    tablai = soupi.find('table', attrs={"id": "ctl00_Contenido_tblDatos"})
    # Extraemos losdatos de las tablas
    for fila in tablai.find_all("tr"):
        k += 1
        a = fila.get_text(separator="s")
        b = a.split("\n")[1]
        c = b.split("s")
        d = c[1:len(c)-1]
        if d[0] != "Fecha":
            d.append(activo)
            d.insert(0, k)
            m3.append(d)

#  Creamos un diccionario
m33 = create_dic(np.array(m3))

#  Creación del csv con los datos históricos de cada activo
nf3 = "./FACT_IBEX35" + ".csv"
df3 = pandas.DataFrame(m33)
df3.to_csv(nf3, sep=',', index=False)

"""
3. EXTRACCIÓN DE LOS DATOS DE LOS MIEMBROS DE LA BOLSA DE MADRID Y SUS LOGOS.
"""

# Realización de la petición.
web3 = "https://www.bolsamadrid.es/esp/aspx/Miembros/Miembros.aspx?tipo=T"
page2 = requests.get(web3, headers=headers)
soup2 = BeautifulSoup(page2.content)
tabla2 = soup2.find('table', attrs={"id": "Tabla"})

# Extración de la lista de los logos.
img11 = soup2.findAll("img")
img = img11[3:37]

url_img = []
# Obtención y guardado de las imágenes de los logos de los miembros
shutil.rmtree('miembros', ignore_errors=True)
os.mkdir("miembros")
for i in range(len(img)):
    b = web + str(img[i]).split("=")[2].split('"')[1]
    r = requests.get(b, stream=True, headers=headers)
    filename = "miembros/" + str(img[i]).split("=")[2].split('"')[1].split("/")[3]
    url_img.append(filename)
    with open(filename, 'wb') as f:
        shutil.copyfileobj(r.raw, f)

# Obtención de los datos de los miembros
k = 0
m2 = [["id","miembro", "direccion", "postal", "telefono", "logo"]]
for fila in tabla2.find_all("td", attrs={"align": "left"}):
    a = fila.get_text(separator="--")
    b = a.split("\n")[0]
    c = list(np.array(b.split("--"))[:4])
    c.append(url_img[k])
    c.insert(0, k+1)
    m2.append(c)
    k += 1
    
m21 = np.array(m2)
m22 = create_dic(m21)

#  Creación del csv de los miembros
nf2 = "./DIM_Miembro" + ".csv"
df2 = pandas.DataFrame(m22)
df2.to_csv(nf2, sep=',', index=False)
