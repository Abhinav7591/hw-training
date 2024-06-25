
url_search = "https://www.migros.ch/onesearch-oc-seaapi/public/v3/search/category"
url_product_urls = 'https://www.migros.ch/product-display/public/v4/product-cards'



category_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    'Origin': 'https://www.migros.ch',
    'Leshopch': 'eyJsdmwiOiJVIiwiZW5jIjoiQTI1NkdDTSIsImFsZyI6ImRpciIsImtpZCI6IjdhZDcwNmZlLWQxODYtNDk1Mi1hYzQzLThkNDMwOTlmMDczNCJ9..qDKSjAPeYRqZbd4Y.LnaAe4vvT4107gHhEzlUc84tDJqSUVNrrNoz9yJVA600WNMWQEheZvtmLDmTkUERkTwWiM91vgnDbCiED-G29cBohCk2hTuxwzJ3M-l5jlHrIwgKWq3lj5YVfT66u7mP_dmRPSM_8B_J_J7jYijdR-aSGDfg7icO2aGsSu8AyvhvkGMGLPFDwlJDHOJhcIfLfJogeGkLZC3rFzhnTW3xmY384PxcvDXinkrSr_TEzQaw4BqgAmXGsDJECn9DwaLRv31ceeM3AuJovj4VEOkyEerUr16ETLEWZl_mXiSgpNY.vx1omoiQr2AILTRRCDaXug',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
}


headers_product_urls = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, zstd',
    'Accept-Language': 'de',
    'Content-Type': 'application/json',
    'Origin': 'https://www.migros.ch',
    'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Linux"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Leshopch': 'eyJsdmwiOiJVIiwiZW5jIjoiQTI1NkdDTSIsImFsZyI6ImRpciIsImtpZCI6IjdhZDcwNmZlLWQxODYtNDk1Mi1hYzQzLThkNDMwOTlmMDczNCJ9..qDKSjAPeYRqZbd4Y.LnaAe4vvT4107gHhEzlUc84tDJqSUVNrrNoz9yJVA600WNMWQEheZvtmLDmTkUERkTwWiM91vgnDbCiED-G29cBohCk2hTuxwzJ3M-l5jlHrIwgKWq3lj5YVfT66u7mP_dmRPSM_8B_J_J7jYijdR-aSGDfg7icO2aGsSu8AyvhvkGMGLPFDwlJDHOJhcIfLfJogeGkLZC3rFzhnTW3xmY384PxcvDXinkrSr_TEzQaw4BqgAmXGsDJECn9DwaLRv31ceeM3AuJovj4VEOkyEerUr16ETLEWZl_mXiSgpNY.vx1omoiQr2AILTRRCDaXug',
}


parser_api_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate,zstd',
    'Accept-Language': 'en',
    'Leshopch': 'eyJsdmwiOiJVIiwiZW5jIjoiQTI1NkdDTSIsImFsZyI6ImRpciIsImtpZCI6IjdhZDcwNmZlLWQxODYtNDk1Mi1hYzQzLThkNDMwOTlmMDczNCJ9..qDKSjAPeYRqZbd4Y.LnaAe4vvT4107gHhEzlUc84tDJqSUVNrrNoz9yJVA600WNMWQEheZvtmLDmTkUERkTwWiM91vgnDbCiED-G29cBohCk2hTuxwzJ3M-l5jlHrIwgKWq3lj5YVfT66u7mP_dmRPSM_8B_J_J7jYijdR-aSGDfg7icO2aGsSu8AyvhvkGMGLPFDwlJDHOJhcIfLfJogeGkLZC3rFzhnTW3xmY384PxcvDXinkrSr_TEzQaw4BqgAmXGsDJECn9DwaLRv31ceeM3AuJovj4VEOkyEerUr16ETLEWZl_mXiSgpNY.vx1omoiQr2AILTRRCDaXug',
    'Migros-Language': 'de',
    'Peer-Id': 'website-js-589.0.0',
    'Priority': 'u=1, i',
    'Sec-Ch-Ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Linux"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Traceparent': '00-0000000000000000332ea3493de4138a-558fbe0fcdfd861e-01',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'X-Correlation-Id': 'bc2d39f0-b605-486b-804e-6be1ca99d65b',
    'X-Datadog-Origin': 'rum',
    'X-Datadog-Parent-Id': '6165355389983229470',
    'X-Datadog-Sampling-Priority': '1'
}
