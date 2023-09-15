from bs4 import BeautifulSoup
from selenium.webdriver import Firefox, FirefoxOptions
import time, environ, pymongo

# # Initialise environment variables
env = environ.Env()
environ.Env.read_env()

def parse_and_save_content(html_content) :

    soup = BeautifulSoup(html_content, 'html.parser')

    criteria = {'class': 'pageComponent', 'data-label': 'SEARCH'}
    cards = soup.find("div", attrs=criteria).find_all("section")

    propertylist = []

    for card in cards :
        
        propertydict = {}

        if card.find("div", {'class' : "srpTuple__cardWrap"}) :
            continue

        propertyName = card.find("a", {'class' : 'projectTuple__projectName'})
        
        if propertyName :
            propertydict['propertyName'] = propertyName.text if propertyName else None
        else :
            break
        
        propertydict['propertyLink'] = propertyName.get('href')
        slidingBoxes = card.find("div", {'class' : 'carousel__slidingBox'}).find_all("div", {'class' : 'configurationCards__cardContainer'})
        propertyType = [slidingBox.find("div", {'class' : 'configurationCards__cardConfigBand'}).text if slidingBox.find("div", {'class' : 'configurationCards__cardConfigBand'}) else None for slidingBox in slidingBoxes]
        propertydict['propertyType'] = propertyType

        propertyPrice = [slidingBox.find("span", {'class' : 'configurationCards__cardPriceHeading'}).text if slidingBox.find("span", {'class' : 'configurationCards__cardPriceHeading'}) else None for slidingBox in slidingBoxes]
        propertydict['propertyPrice'] = propertyPrice

        propertyArea = [slidingBox.find("span", {'class' : 'configurationCards__cardAreaSubHeadingOne'}).text if slidingBox.find("span", {'class' : 'configurationCards__cardAreaSubHeadingOne'}) else None for slidingBox in slidingBoxes]
        propertydict['propertyArea'] = propertyArea

        localities_chip = card.find("div", {'class' : 'SliderTagsAndChips__sliderChipsStyle'})
        localities = localities_chip.find_all('ul', {'class' : 'SliderTagsAndChips__tagsWrap'}) if localities_chip else None
        localities_list = [locality.text for locality in localities] if localities else None
        propertydict['propertyLocality'] = localities_list

        if soup.find("div", attrs={'data-label' : 'CATEGORY'}) :
            propertydict['propertyCity'] = soup.find("div", attrs={'data-label' : 'CATEGORY'}).text

        propertylist.append(propertydict)
    
    return propertylist

def fetchdata() :

    urls = [
    "https://www.99acres.com/search/property/buy/pune?city=19&keyword=Pune&preference=S&area_unit=1&res_com=R",
    "https://www.99acres.com/search/property/buy/delhi?keyword=Delhi&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/mumbai?keyword=Mumbai&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/lucknow?keyword=Lucknow&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/agra?keyword=Agra&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/ahmedabad?keyword=Ahmedabad&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/kolkata?keyword=Kolkata&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/jaipur?keyword=Jaipur&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/chennai?keyword=Chennai&preference=S&area_unit=1&budget_min=0&res_com=R",
    "https://www.99acres.com/search/property/buy/bengaluru?keyword=Bengaluru&preference=S&area_unit=1&budget_min=0&res_com=R&isPreLeased=N"
    ]

    options = FirefoxOptions()
    options.add_argument("--headless")
    driver = Firefox(options=options)

    client = pymongo.MongoClient(env('DB_CONNECTION_STRING'))
    db = client['scheduleddb']
    collection = db['properties']
    
    alldata = []
    for url in urls :
        
        driver.get(url)
        time.sleep(5)
        
        driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        time.sleep(2)

        html_content = driver.page_source

        parsed_data = parse_and_save_content(html_content)

        if parsed_data != [] and parsed_data is not None :
            collection.insert_many(parsed_data)

        alldata.append(parsed_data)
    
    return alldata
