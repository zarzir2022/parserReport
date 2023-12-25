import requests
import pandas as pd

#progressBar
from time import sleep
from tqdm import tqdm

from dlbar import DownloadBar

download_bar = DownloadBar()

#------------------------------
pd.set_option('display.max_rows', None)

def tickerCollector():
    #Данные о тикерах сохраним в переменной file. Так как ссылка не меняется, то можем её захардкодить и не скачивать на компьютер эксель
    url = "https://www.moex.com/ru/listing/securities-list-csv.aspx?type=1"
    fileName = "securities-list-csv.aspx"
    download_bar.download(
        url=url,
        dest = fileName,
        title='Скачиваем тикеры с MOEX...'
        )   
    
    securityList = pd.read_csv("securities-list-csv.aspx", sep=',', encoding='cp1251')
    #headers = securityList.columns
    #Анализируем колонки, которые нам могут помочь. Внимание привлекли колонки TRADE_CODE и INSTRUMENT_CATEGORY
    #Наша задача - отобрать только акции и запомнить их тикеры, чтобы затем узнать текущие цены на инструменты
    #на бирже
    securityList = securityList[["INSTRUMENT_CATEGORY","TRADE_CODE"]]
    #Мы отберем тикеры по двум фильтрам - по слову акции в категориях инструменты
    #И по длине торгового кода - максимальная его не превышает символов (для российсих акций)
    securityList = securityList[
                                (securityList["INSTRUMENT_CATEGORY"].str.contains("акци|Акци"))&
                                (securityList["TRADE_CODE"].str.len()<=6
                                                                           )]
    
    #В итоге мы в реальном времени получили данные о тикерах, которые прямо сейчас торгуются на MOEX
    moexTickersStocks = securityList["TRADE_CODE"].reset_index(drop=True)
    return moexTickersStocks

#Вспомогательная функция подключения к АПИ биржи. Её мы будем вызывать в методах класса AnalizeApi, когда нам потребуется информация с MOEX
def callApi(ticker):
    url = str(f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{ticker}.jsonp?iss.meta=off&iss.json=extended&callback")    
    response = requests.get(url=url)
    response =  response.json()
    return response

#Class, содержащий в себе АПИ-методы
class AnalizeApi():   
    #Создаём возможность задавать всем объектам класса динамический параметр Ticker
    def __init__(self, ticker, year):
        self.ticker = ticker
        self.year = year
    
    
    #Метод, возвращающий информацию обо всех тикерах на бирже c MOEX, принимает на вход Ticker объекта, 
    # который мы присвоили ему ранее. На выход отдаёт информацию о всех торгуемых в данных момент инструментах на бирже
    def get_stocks(self):
        result = callApi(self.ticker)
        return result

    #Метод, возвращающий информацию о конкретной бумаге по тикеру c MOEX, принимает на вход Ticker объекта, 
    # который мы присвоили ему ранее. На выход отдаёт информацию о конкретной бумаге
    def get_stock_info(self):
        result = callApi(self.ticker)
        result = result[1]["securities"][0]
        return result
    
    #Метод, возвращающий информацию об отчётности компании за конкретный год, принимает на вход Ticker объекта и год int, 
    # на выход отдаёт отчётность компании за указанный год
    def get_report(self):
        url = str(f"https://financemarker.ru/api/stocks/MOEX:{self.ticker}/finance")
        period = "Y"
        reportType = "МСФО"
        response = requests.get(url=url)
        reports = response.json()["data"]["reports"]
        filteredReports = list(filter(lambda d: d['year'] == self.year and d['period'] == period and d['type'] == reportType, reports))
        filteredReports = dict((d['year'], d) for d in filteredReports)[self.year]
        return filteredReports
    
    #Метод, возвращающий информацию о рыночной информации за конкретный год, принимает на вход Ticker объекта и год int, 
    # на выход отдаёт фундаментальную статистику по бумаге за указанный год. Помогает найти информацию, которую не отдаёт мосбиржа

    def get_stocks_statistics(self):
        url = str(f"https://financemarker.ru/api/stocks/MOEX:{self.ticker}/finance")
        period = 12
        response = requests.get(url=url)
        shares = response.json()["data"]["shares"]
        filteredShares = list(filter(lambda d: d['year'] == self.year and d['month'] == period and d["code"] == self.ticker, shares))
        filteredShares = dict((d['year'], d) for d in filteredShares)[self.year]
        return filteredShares
    


#Собираем информацию по всем тикерам на бирже, чтобы затем найти их фундаментальные показатели

def parsedReport(moexTickersStocks):
    tickerData = []
    for ticker in tqdm(moexTickersStocks):
        try:
            reportData_2022 = AnalizeApi(ticker = ticker, year = 2022).get_report()
            reportData_2021 = AnalizeApi(ticker = ticker, year = 2021).get_report()
            sharesData = AnalizeApi(ticker = ticker, year = 2022).get_stocks_statistics()
            priceInfo = AnalizeApi(ticker = ticker, year = 2022).get_stock_info()
            
            list = [reportData_2022["code"], #КОД ТИКЕРА
                    priceInfo["PREVPRICE"], #ЦЕНА ТИКЕРА
                    sharesData["num"],  #КОЛИЧЕСТВО АКЦИЙ В ОБРАЩЕНИИ
                    int(reportData_2022["earnings"])*int(reportData_2022["amount"]), #прибыль
                    int(reportData_2022["revenue"])*int(reportData_2022["amount"]), #выручка
                    int(reportData_2022["fcf"])*int(reportData_2022["amount"]), #Free Cash Flow,
                     int(reportData_2022["equity"])*int(reportData_2022["amount"]), #Equity
                    int(reportData_2021["total_assets"])*int(reportData_2021["amount"]), #АКТИВЫ2021
                    int(reportData_2022["total_assets"])*int(reportData_2022["amount"])] #АКТИВЫ2022

        except Exception:
            pass
        else:
            tickerData.append(list)
    return tickerData


#Фильтруем бумаги исходя из колебаний SP500
# def stockTacker(df):
#     df = df[(df["P/E"] >= 6) & (df["P/E"] <= 35)]
#     df = df[(df["P/S"] >= 0.8) & (df["P/S"] <= 3.5)]
#     df = df[(df["P/FCF"] <= 10)]

#     return df

#Анализируем датафрейм, включающий все цены всех интересующих нас акций за 3 года

# def returns():
#     files = glob.glob(r"Price_3Y/*.csv")

#     returnDF = pd.DataFrame()

#     for file in files:
#         data = pd.read_csv(file, sep = ";")

#         ticker = data.iloc[0, data.columns.get_loc('<TICKER>')]
#         first_value = data.iloc[0, data.columns.get_loc('<CLOSE>')]
#         last_value = data.iloc[-1, data.columns.get_loc('<CLOSE>')]
#         result = (last_value-first_value) / last_value

#         returnN = {'TICKER':[ticker], 'RETURN':[result]}
#         returnN = pd.DataFrame(returnN)

#         returnDF = pd.concat([returnDF, returnN])


def main():

    moexTickersStocks = tickerCollector()

    df = pd.DataFrame(data = parsedReport(moexTickersStocks), columns = ["TICKER", "ЦЕНА", "КОЛИЧЕСТВО АКЦИЙ", "ПРИБЫЛЬ", "ВЫРУЧКА", "FCF", \
                                                                         "Собственный капитал (EQUITY)","АКТИВЫ 2021", "АКТИВЫ 2022"])


    #df = stockTacker(df)
    df.to_excel("Акции для анализа.xlsx", index = False)


if __name__ == "__main__":
    main()

