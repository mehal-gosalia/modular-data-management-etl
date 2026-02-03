from faker import Faker

# Map used for ???
faker = Faker()
SyntheticSubMap = {
    "username": lambda: faker.user_name(),
    "name": lambda: faker.name(),
    "address": lambda: faker.address(),
    "email": lambda: faker.email(),
    "birthdate" : lambda: faker.date_of_birth(),
    "date": lambda: faker.date_this_century(),
    "uuid": lambda: faker.uuid4()
}

# Map used for ???
RegexPatternMap = {
    "mask_username": {
        "pattern": r"(.{2})(.*)",
        "replacement": r"\1****"
    },
    "mask_email": {
        "pattern": r"(.{2})(.*)(@.*)",
        "replacement": r"\1****\3"
    },
    "mask_name": {
        "pattern": r"([A-Za-z])",
        "replacement": "*"
    },
    "mask_address_digits": {
        "pattern": r"\d",
        "replacement": "X"
    },
    "mask_all": {
        "pattern": r"(.*)",
        "replacement": "XXX"
    },
    "mask_birthdate": {
        "pattern": r"([0-9]{4})-(..)-(..)",
        "replacement": r"\1-XX-XX"
    },
    "mask_special_plus": {
        "pattern": r"([+])",
        "replacement": "_"
    }
}