import datetime


base_url='https://uae.dubizzle.com'
category_url='https://uae.dubizzle.com/en/property-for-sale/'

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ml;q=0.6',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
}


cookies = {
    'visid_incap_2413658': '7oPmuKDmRCeIcc2gjDuozOCPMGYAAAAAQUIPAAAAAADhWGEW+O2FeS4Nc1suNG95',
    '_gcl_au': '1.1.447067460.1714458597',
    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22cmNiMk0oLrTFvyKSUMhF%22%7D',
    '_scid': 'ca7cb114-c32e-4a79-9c3d-32b3e614156c',
    'USER_DATA': '%7B%22attributes%22%3A%5B%5D%2C%22subscribedToOldSdk%22%3Afalse%2C%22deviceUuid%22%3A%22ddcf5883-aa4f-463a-bf0e-0c8da9efef67%22%2C%22deviceAdded%22%3Atrue%7D',
    '_fbp': 'fb.1.1714458633076.901677530',
    '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%221839328923.1714458597%22%7D',
    'csrftoken': 'wEJCt8oENp6q00brXADkvaa1Cl2KrUfog6m8XaMC6vSUYWyFDlogEVBRyxKl59Ol',
    '_cc_id': '9146562d72d543b3038fff311da7b14d',
    'ias': '0',
    '_tt_enable_cookie': '1',
    '_ttp': 'zX9ZiKBcEXMeFlB9V7zBqXAX95f',
    '_ScCbts': '%5B%22172%3Bchrome.2%3A2%3A5%22%5D',
    '_sctr': '1%7C1720117800000',
    'HARD_ASK_STATUS': '%7B%22actualValue%22%3A%22prompt%22%2C%22MOE_DATA_TYPE%22%3A%22string%22%7D',
    '_gid': 'GA1.2.1031144563.1720409370',
    'SOFT_ASK_STATUS': '%7B%22actualValue%22%3A%22shown%22%2C%22MOE_DATA_TYPE%22%3A%22string%22%7D',
    'sid': 'tjz28t6v9lakr96w7pgda5g6n91lp38g',
    'nlbi_2413658': 'yuPJJg+5EA15PsZ9eguELgAAAADH053M7OT2VltDGaYYbvXN',
    'incap_ses_50_2413658': '9X/wWCwGJSeyEDAk2KKxAOFRj2YAAAAArTOgSx697q2uYIBJKuhz3g==',
    'panoramaId_expiry': '1720755044091',
    'panoramaId': 'ab8bda0658b1f7b0a78fcae06f97a9fb927a58d7400b87f55139987f7f877b87',
    'panoramaIdType': 'panoDevice',
    'moe_uuid': 'ddcf5883-aa4f-463a-bf0e-0c8da9efef67',
    'OPT_IN_SHOWN_TIME': '1720679726405',
    'cto_bundle': '3b6kpl90OGNTR1ZnNWhaQ1VYTXNIaGdPWUZQMG1JbTBXaEhERWZ5NFcySElnZURweHV0cHglMkZQYUEzaWN4VWl4RTBXUSUyRklCTG9JdmpmM0p3MGdzUFUlMkJKbDNpOFJmbmNIamJ5SjIyR0pGR2ZMaHhTaUJRN3ZBVUdzM1pKVkJsM3NRSGMzbUpPTHNWTzY2VW1qcGQ4MjRYRGhBdTElMkJYQWVsM2tQZCUyRmRKJTJGJTJCcmJUYUZJY2lSJTJGajdDWWxOMXZqeUltaVVDMm1neTB5TFZaQ0VLZzVSTXd3amxNcnZpZyUzRCUzRA',
    '_ga_LRML1YM9GH': 'GS1.1.1720690947.21.1.1720693084.60.0.0',
    '_ga': 'GA1.1.1839328923.1714458597',
    '_scid_r': 'ca7cb114-c32e-4a79-9c3d-32b3e614156c',
    'SESSION': '%7B%22sessionKey%22%3A%22a8d3a791-e305-4d4c-8585-427065f1c815%22%2C%22sessionStartTime%22%3A%222024-07-11T09%3A42%3A29.035Z%22%2C%22sessionMaxTime%22%3A1800%2C%22customIdentifiersToTrack%22%3A%5B%5D%2C%22sessionExpiryTime%22%3A1720694884349%2C%22numberOfSessions%22%3A20%7D',
    '__gads': 'ID=d966f0bb80181f72:T=1714460400:RT=1720693084:S=ALNI_Mb2SvyHcYUmpzuDVWL_CwKV8sqyow',
    '__gpi': 'UID=00000e00e5bda1a9:T=1714460400:RT=1720693084:S=ALNI_Ma5WlgmgAFLtWy1lqZYupWYp3v7gg',
    '__eoi': 'ID=1b5bf13d63c9ba37:T=1714458598:RT=1720693084:S=AA-AfjZ1ehHG-FOs6byda7wU5vXb',
    'nlbi_2413658_2147483392': 'PA7NcZxiODn6XgxOeguELgAAAADoyvTP5v3H9jheJKwsz/R5',
    'reese84': '3:LRIi8ZaZAlWd7s1KM9sGxw==:lNjcNAmlAfEv/FbYeQMwO5qxBMwarEonLBz/NEQjWUP/uySd14SXPWdOU1YAkM52f8TkUnfVk/JHmaLPa8QQQZE9R1M0YK6cPHEjcRMABHOJr3+fADwRPQEeZwcXOEb0t1nqCwnjL9HuIsbKxTDRkSxV6p7LGF0QV0uSNNqL6+oYVt4xMkmzdphgzJR81NqUqznfCytzuXsUw2di8aQwXIrf6IPWB3hhLIYMZZbmqAUqVHL4S8kpsbuS+L5wsUbkm/dwsEjqzn0S6UmjUZ7sca4u5Dj7T7o14pLJyzDVTLD439HvVleCRIRPzElP808vu9E+UI5vwSSdNpfj2nkGG21NzOo40ByVaMzVNFv9stcpHCXQnJLlE8t38SB4JnEhvol1PD9ODmZULZAaP9C/eicPhvypL3zQ/3DLm0oRicsvtufI8jNqHcoMHXh9dG9YpYIZlenE/vPMN9LpoqaCqA==:oXcD3yVaI57qsgcPZBQl3y+4k2HPRXJa5qiWM23MVpE=',
}

today = datetime.date.today()
days_until_thursday = (3 - today.weekday() + 7) % 7
next_thursday_date = today + datetime.timedelta(days=days_until_thursday)
next_thursday = next_thursday_date.strftime("%Y-%m-%d")