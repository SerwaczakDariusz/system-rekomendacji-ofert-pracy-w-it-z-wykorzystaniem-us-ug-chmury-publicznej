import json
import re
import time
from collections import Counter
from functools import reduce

import emoji
import spacy
from connectors.mongodb import MongoDBConnector
from googletrans import Translator


def remove_emoji(string):
    return emoji.replace_emoji(string, "")


def capitalize_only_first_one(
    string,
):  # Model nie łapie słów takich jak "Agile" vs "agile" albo "C++" vs "c++"
    return string[:1].upper() + string[1:]


def remove_parenthesis(string):
    return str.join("", re.split("[()]", string))


def remove_all_numbers_and_versions(string):
    return str.join("", re.split(r"\.[x0-9]|[0-9]\+|[0-9/]{2}|[0-9]", string))


# Load data from the MongoDB collection
connector = MongoDBConnector()
connector.set_database("jobOfferDB")
connector.set_collection("processed")
data = list(connector.read_all())


with open("tags.txt", "w") as f:
    tags = reduce(lambda x, y: x + y, [entry["skills"] for entry in data], [])
    tags = [tag["name"].strip() for tag in tags]
    counter = Counter(tags)
    with open("counter.txt", "w") as cnt_file:
        cnt_file.write(json.dumps(counter, indent=4))
    tags = list(set(tags))
    print("tags ==> ", len(tags))
    f.write(json.dumps(tags, indent=4))


# init the Google API translator
translator = Translator()

start = time.time()
with open("tags.txt", "r") as f:
    translated = dict()
    translations = translator.translate(list(set(data)))
    for index, translation in enumerate(translations):
        translation_text = remove_emoji(translation.text)

        # Remove weird unicodes
        string_encode = translation_text.encode("ascii", "ignore")
        translation_text = string_encode.decode()

        translation_origin = remove_emoji(translation.origin)
        if translation_text in translated:
            translated[translation_text].append(translation.origin)
        else:
            translated[translation_text] = [translation.origin]

        print(index, translation_origin, " --> ", translation_text)

print("translated ==> ", len(translated))
end = time.time()

with open("translated.txt", "w") as f:
    f.write(json.dumps(translated, indent=4))

output = {}

# https://spacy.io/models#conventions
nlp = spacy.load("en_core_web_lg")

nlp.vocab[
    "+"
].vector *= 0  # Samotny plus jest bardzo mocnym znakiem, zakloca mocno wyniki

start2 = time.time()
threshold = 0.98
index = 0

ordered_translations = list(reversed(sorted(translated, key=lambda x: counter[x])))
while ordered_translations and (title := ordered_translations.pop(0)):
    print(index, title)
    index += 1
    title_reference = nlp(
        remove_parenthesis(
            remove_all_numbers_and_versions(capitalize_only_first_one(title))
        )
    )
    for key, value in output.items():
        cluster_reference = nlp(
            remove_parenthesis(
                remove_all_numbers_and_versions(capitalize_only_first_one(key))
            )
        )
        similarity = cluster_reference.similarity(title_reference)
        if similarity >= threshold:
            output[key].append((title, similarity))
            break
        elif key.upper() == title.upper():
            output[key].append((title, similarity))
            break
        elif not similarity and key.upper() in re.split(
            "[0-9]|,| |;|/|\t|\n|\|", title.upper()
        ):
            # Trik, który pozwala nam dopasowywać słowo nawet jezeli nie istnieje w słowniku modelu,
            # np Python3, PHP7, C/C++ itp. Te słowa nie istnieją dla tego modelu wiec podopienstwo to 0, za to znormalizowane
            # slowo bedzie rowne jednemu z podciagow np. Python, C, C++ albo PHP.
            # nie mozemy sprawdzac samego podciagu bo np C zawiera się w wielu słowach np. CQRS
            output[key].append((title, similarity))
            break
    else:
        output[title] = []

    if len(output) % 100 == 0:
        with open("tag_buckets.json", "w") as f:
            f.write(json.dumps(output, indent=4))

end2 = time.time()
print("Similarity calculation took: ", end2 - start2, " seconds")
print("Ilosc bucketow: ", len(output))

with open("tag_buckets.json", "w") as f:
    f.write(json.dumps(output, indent=4))
