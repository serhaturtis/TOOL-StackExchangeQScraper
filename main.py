import requests
import os
import sys
import time
from bs4 import BeautifulSoup
import json
import csv
import threading
from functools import partial

threads = []
output_folder = "./downloads"

def get_urls_to_scrape(file_path):
    with open(file_path, mode="r", newline="", encoding="utf-8") as csvfile:
        csvreader = csv.DictReader(csvfile)
        return [row for row in csvreader]

def save_data(data_filepath, data):
    with open(data_filepath, "a", encoding="utf-8") as file:
        json.dump(data, file)
        file.write("\n")

def runner_thread_handler(url, tag):
    url_to_scrape = url + "/questions/"
    data_filepath = f"{output_folder}/{tag}_data.jsonl"
    last_filepath = f"{output_folder}/{tag}_last"

    if not os.path.exists(last_filepath):
        with open(last_filepath, "w", encoding="utf-8") as file:
            file.write("1")
    last = int(open(last_filepath, "r").read())

    for n in range(last, 2000000):
        with open(last_filepath, "w", encoding="utf-8") as index_file:
            index_file.write(str(n))

        req = requests.get(url_to_scrape + str(n))
        soup = BeautifulSoup(req.content, "html.parser")
        l = soup.find_all("title")
        m = soup.find_all("div", {"class": "s-prose js-post-body"})

        if not l:
            continue

        title = l[0].text
        if m:
            answer = m[1].text if len(m) > 1 else ""
            if "Page not found" not in title:
                print(f"Saving data for {tag}: {title}")
                save_data(data_filepath, {title: answer})

        time.sleep(5)

def main():
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    sites_to_scrape = get_urls_to_scrape("sites_to_scrape.csv")
    for item in sites_to_scrape:
        url = item["url"]
        tag = item["tag"]
        thread = threading.Thread(target=partial(runner_thread_handler, url, tag), daemon=True)
        threads.append(thread)

    # start threads
    for thread in threads:
        thread.start()

    # wait for threads to join
    for thread in threads:
        thread.join()

    sys.exit(0)

if __name__ == "__main__":
    main()
