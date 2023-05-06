import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json
import time
URL = "https://api.hh.ru/vacancies"


def get_vacancy(vacancy_id: str):
    url = f"{URL}/{vacancy_id}"
    while True:
        response = requests.api.get(url)
        if response.ok:
            break
        elif response.json()["errors"][0]["type"] == "not_found":
            return None
        else:
            time.sleep(21 * 60)

    vacancy = response.json()

    return vacancy

def collect_vacancies(id):
    target_url = URL + "?area=1342&per_page=50&professional_role=" + str(id)
    num_pages = requests.get(target_url).json()["pages"]

    ids = []
    not_found = False
    for idx in range(num_pages + 1):
        while True:
            response = requests.get(target_url, {"page": idx})
            if response.ok:
                break
            elif response.json()["errors"][0]["type"] == "not_found":
                not_found = True
                break
            else:
                time.sleep(21 * 60)

        if not_found:
            break
        data = response.json()
        ids.extend(x["id"] for x in data["items"])

    jobs_list = []
    with ThreadPoolExecutor(max_workers=1) as executor:
        for vacancy in tqdm(
                executor.map(get_vacancy, ids),
                desc="Get data via HH API",
                ncols=100,
                total=len(ids),
        ):
            jobs_list.append(vacancy)

    return jobs_list

def main():
    f_roles = open("unique_prof.json", "r")
    roles = json.load(f_roles)
    for role in roles:
        if role["id"] not in ("8", "9", "18"):
            f_role = open(f"vacancies/{role['id']} {role['name']}.json", "w", encoding="utf8")
            json.dump(collect_vacancies(role["id"]), f_role, indent=2, ensure_ascii=False)



if __name__ == "__main__":
    main()