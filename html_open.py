"""
Открывает HTML-отчёты бэктестов (sentiment_backtest) в новом окне Google Chrome.
"""

# import webbrowser
# import os
# import time

# # Список файлов
# files = [
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\buhinvest_analize\pl_buhinvest_interactive.html",
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\rts\plots\strategy_analysis.html",
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\mix\plots\strategy_analysis.html",
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\br\plots\strategy_analysis.html",
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\gold\plots\strategy_analysis.html",
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\ng\plots\strategy_analysis.html",
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\si\plots\strategy_analysis.html",
#     r"C:\Users\Alkor\VSCode\pj14_rss_news_oil_gaz_embeded\spyf\plots\strategy_analysis.html",
# ]

# for file in files:
#     path = os.path.abspath(file)
#     url = f"file:///{path.replace(os.sep, '/')}"
    
#     print(f"[OPEN] {url}")
#     webbrowser.open_new_tab(url)
    
#     time.sleep(0.3)  # небольшая пауза, чтобы вкладки не слипались


import subprocess

# Список файлов
files = [
    r"C:\Users\Alkor\VSCode\pj16_sentiment_gaz_oil_trade\rts\plots\sentiment_backtest.html",
    r"C:\Users\Alkor\VSCode\pj16_sentiment_gaz_oil_trade\mix\plots\sentiment_backtest.html",
]

chrome = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

subprocess.Popen([chrome, "--new-window"] + files)
