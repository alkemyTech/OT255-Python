my_dict = {
    "cuatro": 4,
    "tres": 3,
    "seis": 6,
    "nueve": 9,
    "siete": 7
}

data = list(my_dict)

print(data)

data.sort(key=lambda x: x.get("value"))

print(data)
