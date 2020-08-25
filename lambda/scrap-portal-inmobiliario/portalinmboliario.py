import pandas as pd
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

def get_text_by_css_selector(e, selector):
    try:
        return e.find_element_by_css_selector(selector).text
    except  NoSuchElementException:
        return ''


def process_elems(elems):

    records = []

    for e in elems:
        id_publicacion = e.get_attribute('id')

        url = e.find_element_by_css_selector('.image-content a').get_attribute('href').split('#')[0]

        image_src = e.find_element_by_css_selector('.image-content img').get_attribute('data-src')
        if image_src is None:
            image_src = e.find_element_by_css_selector('.image-content img').get_attribute('src')

        image_label = get_text_by_css_selector(e, '.item__image-label')

        price_text = get_text_by_css_selector(e,'.item__price')
        price_symbol = get_text_by_css_selector(e,'.price__symbol')

      

        price_fraction = get_text_by_css_selector(e,'.price__fraction').replace('.','')

        if price_symbol == '$':
            price_fraction = int(int(price_fraction) / 28600)
            price_symbol = 'UF'

        price_since = get_text_by_css_selector(e,'.price__since')

        info = get_text_by_css_selector(e,"div.item__attrs")

        info_elems = info.split("|")
        dormitorios = None
        banos = None
        area = None
        for info in info_elems:
            if 'dormitorio' in info:
                dormitorios = info.strip().split(' ')[0]
            elif 'baño' in info:
                banos = info.strip().split(' ')[0]
            elif 'm²' in info:
                area = info.strip().split(' ')[0]

        title = e.find_element_by_css_selector("span.main-title").text.replace(";",",")
        subtitle = e.find_element_by_css_selector(".item_subtitle p").text

        record = {
            'id_publicacion':  id_publicacion,
            'url_publicacion': url,
            'image_url' :      image_src,
            'image_label':     image_label,
            'price_text' :     price_text,
            'price_symbol':    price_symbol,
            'price_value':     price_fraction,
            'price_since':     price_since,
            'area':            area,
            'dormitorios':     dormitorios,
            'banos':           banos,
            'title':           title,
            'subtitle':        subtitle,
        }

        records.append(record)

    return records


def resultados(driver, operacion='venta',categoria='venta_casa', busqueda='vitacura', min_bedrooms = 2, max_bedrooms=4, min_bathrooms = 2, max_price_uf =16000, min_price_uf=5000,min_m2=120):

    #Primero seleccionamos la operacion (venta, arriendo)
    driver.find_element_by_id('operations-dropdown').find_element_by_class_name('searchbox-dropdown__content').click()
    driver.find_element_by_css_selector('li[data-id="{}"]'.format(operacion)).click()

    #Ahora seleccionamos la operacion (venta_casa, arriendo_casa, etc)
    driver.find_element_by_id('categories-dropdown').find_element_by_class_name('searchbox-dropdown__content').click()
    driver.find_element_by_css_selector('li[data-id="{}"]'.format(categoria)).click()

    #Ahora seleccionamos la comuna
    where = driver.find_element_by_class_name('searchbox-autocomplete__field')
    where.clear()
    where.send_keys(busqueda) #esto desplega una lista de autocompletado con sugerencias
    driver.find_element_by_css_selector('li.searchbox-autocomplete-list__item').click() #seleccionamos la primera opcion (que será la que escribimos)
    driver.find_element_by_id('search-submit').click() #Buscar



    driver.find_element_by_class_name("filters__price").find_element_by_css_selector('label[for="CLF"').click()
    
    driver.find_element_by_id('fromPrice').send_keys(min_price_uf) 
    driver.find_element_by_id('toPrice').send_keys(max_price_uf) 
    driver.find_element_by_class_name("filters__price").find_element_by_class_name("price-filter__actions").click()


    bedrooms_filter = driver.find_element_by_css_selector("#id_BEDROOMS")
    bedrooms_filter.find_element_by_css_selector("#fromRange").send_keys(min_bedrooms) 
    bedrooms_filter.find_element_by_css_selector("#toRange").send_keys(max_bedrooms)
    bedrooms_filter.find_element_by_css_selector("button.filter-action-btn").click()


    bathroom_filter = driver.find_element_by_css_selector("#id_FULL_BATHROOMS")
    bathroom_filter.find_element_by_css_selector("#fromRange").send_keys(min_bathrooms) 
    bathroom_filter.find_element_by_css_selector("button.filter-action-btn").click()

    m2_filter = driver.find_element_by_css_selector("#id_COVERED_AREA")
    m2_filter.find_element_by_css_selector("#fromRange").send_keys(min_m2)
    m2_filter.find_element_by_css_selector("button.filter-action-btn").click()

    records  = []
    end_pages = False

    while not end_pages:

        elems = driver.find_elements_by_css_selector("li.results-item > div")
        driver.implicitly_wait(0)
        records += process_elems(elems)

        try:
            driver.implicitly_wait(15)
            driver.find_element_by_css_selector('.andes-pagination__button--next').click() #vamos a la siguiente página.
        except:
            end_pages = True
            print ('Fin Paginas')
            break
    

    return pd.DataFrame.from_records(records)

    

