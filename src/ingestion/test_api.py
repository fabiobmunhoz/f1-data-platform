import requests


url = "https://api.jolpi.ca/ergast/f1/2025/driverstandings/"

response = requests.get(url, timeout=30)

print("Status:", response.status_code)

data = response.json()

standings_lists = data["MRData"]["StandingsTable"]["StandingsLists"]

if standings_lists:
    drivers = standings_lists[0]["DriverStandings"]

    print("Quantidade de pilotos:", len(drivers))
    print()

    for item in drivers[:10]:

        driver = item["Driver"]
        constructors = item.get("Constructors", [])

        nome = f'{driver["givenName"]} {driver["familyName"]}'
        nacionalidade = driver.get("nationality", "Unknown")

        equipes = ", ".join(
            constructor["name"]
            for constructor in constructors
        )

        print(
            nome,
            "|",
            nacionalidade,
            "|",
            equipes
        )