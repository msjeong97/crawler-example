import pandas as pd

from bs4 import BeautifulSoup

FILE_PATH = "raw-device-info.csv"


def main():
    df_already_crawled_html = pd.read_csv(FILE_PATH)

    model_names = []
    statuses = []
    oss = []
    models = []
    prices = []

    for index, row in df_already_crawled_html.iterrows():
        html_doc = row['html']
        soup = BeautifulSoup(html_doc, 'html.parser')

        model_name_tag = soup.find('h1', {'data-spec': 'modelname'})
        model_name = model_name_tag.get_text(strip=True)
        model_names.append(model_name)

        status_element = soup.find('td', {'data-spec': 'status'})
        status_value = status_element.get_text(strip=True) if status_element else None
        statuses.append(status_value)

        os_element = soup.find('td', {'data-spec': 'os'})
        os_value = os_element.get_text(strip=True) if status_element else None
        oss.append(os_value)

        model_element = soup.find('td', {'data-spec': 'models'})
        model_value = model_element.get_text(strip=True) if status_element else None
        models.append(model_value)

        price_element = soup.find('td', {'data-spec': 'price'})
        price_value = price_element.get_text(strip=True) if status_element else None
        prices.append(price_value)

    df = pd.DataFrame({
        'model_name': model_names,
        'status': statuses,
        'os': oss,
        'model': models,
        'price': prices
    })

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(df)


if __name__ == "__main__":
    main()
