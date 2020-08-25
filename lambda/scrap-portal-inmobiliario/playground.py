#%%
import os
from selenium import webdriver
import pandas as pd
#from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#%%
#driver = webdriver.Firefox(executable_path=GeckoDriverManager().install())
driver = webdriver.Chrome(ChromeDriverManager().install())
driver.implicitly_wait(15)
driver.get("https://www.portalinmobiliario.com")
operacion = 'venta'
categoria = 'venta_casa'
busqueda = 'lo barnechea'


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
driver.find_element_by_id('toPrice').send_keys("16000") 
driver.find_element_by_class_name("filters__price").find_element_by_class_name("price-filter__actions").click()


#price_filter = driver.find_element_by_css_selector("#id_price")
#price_filter.find_element_by_css_selector("input[type='radio'][value='CLF']").click()




#price_filter = driver.find_element_by_css_selector("#id_price")
#max_price_field = price_filter.find_element_by_css_selector("#toPrice")



#price_filter.find_element_by_css_selector("#fromPrice").set_attribute("value","16000")

#price_filter.find_element_by_css_selector("#CLF").click()




#ActionChains(driver).move_to_element(price_filter).perform()
#ActionChains(driver).move_to_element(max_price_field).send_keys("16000").perform()


#max_price_field.send_keys("16000")
#price_filter.find_element_by_css_selector("#CLF").click()


#driver.find_element_by_class_name("filters__BEDROOMS").find_element_by_class_name("custom-range-filter__value").send_keys("2") 
#driver.find_element_by_class_name("filters__BEDROOMS").find_element_by_class_name("custom-range-filter__actions").click()


#elems = driver.find_elements_by_css_selector("li.results-item > div")


# %%
driver.close()
driver.quit()

# %%

# %%
