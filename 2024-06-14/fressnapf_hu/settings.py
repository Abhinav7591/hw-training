import datetime

sitemap_urls = [
    "https://webshop.fressnapf.hu/product-sitemap.xml",
    "https://webshop.fressnapf.hu/product-sitemap2.xml",
    "https://webshop.fressnapf.hu/product-sitemap3.xml",
    "https://webshop.fressnapf.hu/product-sitemap4.xml",
    "https://webshop.fressnapf.hu/product-sitemap5.xml",
    "https://webshop.fressnapf.hu/product-sitemap6.xml"
]


headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ml;q=0.6",
    "If-None-Match": '"dda1f-qsY9kvw0LRKNqImtxdKscrDn4Wo-gzip"',
    "Priority": "u=1, i",
    "Sec-Ch-Ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Linux"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "no-cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

today = datetime.date.today()
days_until_thursday = (3 - today.weekday() + 7) % 7
next_thursday_date = today + datetime.timedelta(days=days_until_thursday)
next_thursday = next_thursday_date.strftime("%Y-%m-%d")
