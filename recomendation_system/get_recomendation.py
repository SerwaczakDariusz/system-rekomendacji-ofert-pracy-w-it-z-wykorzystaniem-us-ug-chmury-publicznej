import base64
import json
from copy import deepcopy
from functools import reduce
from io import BytesIO
from math import dist

import dash_bootstrap_components as dbc
import pandas as pd
from connectors.mongodb import MongoDBConnector
from dash import Dash, Input, Output, dcc, html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from gensim.utils import tokenize
from pdfminer.high_level import extract_text

# Translation mapper
with open("translated.txt", "r") as f:
    translation_buckets = json.load(f)

translation_mapper = {}
for k, v in translation_buckets.items():
    translation_mapper[k] = k
    for item in v:
        translation_mapper[item] = k

labels = {
    1: "Nowicjusz (ang. Wannabe)",
    2: "Początkujący",
    3: "Średniozaawansowany",
    4: "Zaawansowany",
    5: "Ekspert",
}

with open("translation_mapper.json", "w") as f:
    json.dump(translation_mapper, f, indent=4)


# Get CV tags
def get_cv_tags(pdf_data):
    popularity_ranking = dict(zip(tag_buckets.keys(), range(len(tag_buckets))))
    keywords = list(
        set(
            reduce(
                lambda x, y: x + y,
                list(translation_buckets.values()) + [list(translation_buckets.keys())],
                [],
            )
        )
    )

    # "otwieramy" plik PDF, a w rzeczywistosci wczytujemy strumien IO
    pdf_data = base64.b64decode(pdf_data.replace("data:application/pdf;base64,", ""))
    pdf_stream = BytesIO(pdf_data)

    # ekstrakcja tekstu
    text = extract_text(pdf_stream)

    # usuwamy zbędne białe znaki (np. spacje, nowe linie) i zapisujemy do pliku .txt
    content = text.strip()

    # Remove weird unicodes
    string_encode = content.encode("ascii", "ignore")
    content = string_encode.decode()

    findings = set()
    tokens = [token.lower() for token in tokenize(content)]

    for keyword in keywords:
        if keyword.lower() in tokens:
            findings.add(tag_mapper[translation_mapper[keyword]])

    pdf_stream.close()
    return sorted(list(findings), key=lambda x: popularity_ranking[x])


# Load tags
with open("tag_buckets.json", "r") as f:
    tag_buckets = json.load(f)
tags = list(tag_buckets.keys())

tag_mapper = {}
for k, v in tag_buckets.items():
    tag_mapper[k] = k
    for item in v:
        tag_mapper[item[0]] = k

with open("tag_mapper.json", "w") as f:
    json.dump(tag_mapper, f, indent=4)

# Init the app
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])

# komponent do wyboru tagów
tag_dropdown = dcc.Dropdown(
    id="tag-dropdown",
    options=[{"label": tag, "value": tag} for tag in tags],
    style={"width": "70%"},
    multi=True,
    placeholder="Lub wybierz umiejętności ręcznie",
)

# komponent do przechowywania wybranych tagów i wartości
store = dcc.Store(id="store")


# funkcja do aktualizacji wybranych tagów i wartości
@app.callback(
    Output("store", "data"),
    Input("tag-dropdown", "value"),
    State("value-container", "children"),
)
def update_data(values, children):
    if not values:
        raise PreventUpdate

    data = {}

    current_keys = []
    current_values = []
    for c in children:
        for x in c["props"]["children"]:
            if "children" in x["props"]:
                current_keys.append(x["props"]["children"])
            else:
                current_values.append(x["props"]["value"])
    currecnt_data = dict(zip(current_keys, current_values))

    for key in currecnt_data:
        data[key] = currecnt_data[key]

    for value in values:
        if value not in data:
            data[value] = 1

    for data_key in deepcopy(list(data.keys())):
        if data_key not in values:
            del data[data_key]

    return data


@app.callback(Output("seniorities-output", "children"), [Input("seniorities", "value")])
def filter_based_on_seniority(value):
    global data
    if not value:
        raise PreventUpdate

    data = bucked_data[value]
    return ""


# funkcja do generowania komponentów do wyboru wartości dla wybranych tagów
# oraz do usuwania wartości dla odznaczonych tagów
@app.callback(Output("value-container", "children"), Input("store", "data"))
def update_value_inputs(data):
    if data is None:
        return []
    inputs = []

    for tag in data.keys():
        input_element = html.Div(
            [
                html.Label(tag, style={"width": "260px", "margin-right": "10px"}),
                dcc.Dropdown(
                    id={"type": "value-input", "index": tags.index(tag)},
                    options=[{"label": labels[i], "value": i} for i in range(1, 6)],
                    value=data[tag],
                    clearable=False,
                    searchable=False,
                    style={"width": "240px"},
                ),
            ],
            style={"display": "flex"},
        )
        inputs.append(input_element)

    return inputs


@app.callback(
    Output("loading-text", "children"),
    Output("tag-dropdown", "value"),
    Input("upload-data", "contents"),
    State("upload-data", "filename"),
)
def get_tags_from_uploaded_cv(content, name):
    if not content or not name:
        raise PreventUpdate

    tags = get_cv_tags(content)[
        :15
    ]  # Get top 15 tags from candidate CV (by popularity)
    return html.Strong(name, style={"color": "green"}, id="loading-text"), tags


# Add controls to build the interaction
@app.callback(
    Output("table-container", "children"),
    [Input("submit-button", "n_clicks")],
    [State("value-container", "children")],
)
def recommend(n_clicks, children):
    print("DATA COUNT:", len(data))
    ###
    current_keys = []
    current_values = []
    for c in children:
        for x in c["props"]["children"]:
            if "children" in x["props"]:
                current_keys.append(x["props"]["children"])
            else:
                current_values.append(x["props"]["value"])
    user = dict(zip(current_keys, current_values))
    ###

    if n_clicks <= 0 or not user:
        raise PreventUpdate

    print("\nRecommend for:", user)

    logic_or = (
        lambda pair: pair[1] - 0.1
        if (not pair[0] and pair[1])
        else (pair[0] or pair[1])
    )
    # LateX: f(x_o, x_u) = \begin{cases} x_u - 0.1 & \text{jeżeli, } x_o = 0\wedge x_u \neq 0  \\ x_o & \text{w przeciwnym razie} \end{cases}

    # 1. Vectorization
    base_vector = {tag: [0, 0, 0, 0, 0] for tag in tag_buckets}
    user_vector = deepcopy(base_vector)
    print("USER:\n")
    for skill, level in user.items():
        user_vector[skill] = ([1] * level) + ([0] * (5 - level))
        print((skill, level), "\t\t", user_vector[skill])

    # 1.1 Flatten for the user's vector
    real_user_vector = []
    for key in user_vector:
        real_user_vector.extend(user_vector[key])

    import time

    start = time.time()

    recomendations = []

    print("\nOFFERS:\n")
    job_offer_vectors = {}
    for index, item in enumerate(data):
        vector = deepcopy(base_vector)

        # Fill vector with base values
        for skill in item["normalized_skills"]:
            translation = translation_mapper[skill["name"].strip()]
            real_name = tag_mapper[translation]

            # Wektoryzacja oferty
            vector[real_name] = ([1] * skill["level"]) + ([0] * (5 - skill["level"]))
            # Podbicie umiejętności oferty o umiejętności usera czyli Java 1 staje się Java 3, jezeli User ma Java 3 w CV
            vector[real_name] = [
                logic_or(pair)
                for pair in zip(vector[real_name], user_vector[real_name])
            ]
        # Fikcyjne ustawienie 1. bitu na 1, jezeli user ma nadmarowa umiejetnosc, pozwoli to podbic wyzej oferty bez 1 umiejetnosci, niz te wymagajace jednej extra umiejetnosci itp.
        for user_skill in user:
            vector[user_skill][0] = vector[user_skill][0] or 0.25

        # 1.2 Flatten for the offer's vector
        real_job_offer_vector = []
        for key in user_vector:
            real_job_offer_vector.extend(vector[key])

        job_offer_vectors[item["_id"]] = real_job_offer_vector
        distance = dist(job_offer_vectors[item["_id"]], real_user_vector)
        skills_text = html.Ul(
            [
                html.Li(f"{el['name']}: {labels[el['level']]}")
                for el in item["normalized_skills"]
            ]
        )
        offer_url = html.P(
            html.A(item["title"], href=item["url"], style={"font-weight": "500"})
        )
        recomendations.append(
            {
                "_id": item["_id"],
                "Link do oferty": offer_url,
                "Portal": item["source"],
                "Wymagania": skills_text,
                "Dopasowanie": round(distance, 4),
            }
        )
        if not index % 500:
            print("Index: ", index)

    end = time.time()
    recomendations_count = 20

    print("Recomendation finding took: ", end - start, " seconds")
    sorted_recomendations = sorted(recomendations, key=lambda x: x["Dopasowanie"])

    # tworzenie tabeli z DataFrame
    final_recomendation = deepcopy(sorted_recomendations[:recomendations_count])
    for item in final_recomendation:
        del item["_id"]
    frame = pd.DataFrame(final_recomendation)
    frame.columns = [col.capitalize() for col in frame.columns]

    table_header = [
        html.Thead(
            html.Tr(
                [
                    html.Th(column_name, className="text-center")
                    for column_name in frame.columns
                ],
                className="container text-center",
            )
        )
    ]
    rows = []
    for item in frame.to_dict("records"):
        row = []
        for key in item:
            row.append(html.Td(item[key]))
        rows.append(html.Tr(row))
    table_body = [html.Tbody(rows)]
    # zwracanie tabeli
    return dbc.Table(
        table_header + table_body, bordered=True, striped=True, color="primary"
    )


# Run the app
if __name__ == "__main__":
    # Load data from the MongoDB collection
    connector = MongoDBConnector()
    connector.set_database("jobOfferDB")
    connector.set_collection("processed")
    raw_data = list(connector.read_all())
    ### Dirty hack
    rewritted_raw_data = []
    unique_offers = set()
    for entry in raw_data:
        if (
            entry["source"] == "justjoin"
            and "-".join(entry["_id"].split("-")[:-1]) not in unique_offers
        ):
            rewritted_raw_data.append(entry)
            unique_offers.add("-".join(entry["_id"].split("-")[:-1]))
        elif entry["source"] != "justjoin":
            rewritted_raw_data.append(entry)
    raw_data = rewritted_raw_data
    ###

    ### Filtracja workaround
    levels = set()
    for d in raw_data:
        level = d["experience_level"]
        if isinstance(level, list):
            for el in level:
                levels.add(el.upper())
        else:
            levels.add(level.upper())

    levels = list(levels)
    for index, l in enumerate(levels):
        if "MID" in l or "MEDIUM" in l:
            levels[index] = "MID"
        elif (
            "JUNIOR" in l
            or "TRAINEE" in l
            or "STAŻYSTA" in l
            or "PRAKTYKANT" in l
            or "ASYSTENT" in l
        ):
            levels[index] = "JUNIOR"
        elif "SENIOR" in l or "EXPERT" in l or "EKSPERT" in l:
            levels[index] = "SENIOR"
        elif "PREZES" in l or "DYREKTOR" in l or "KIEROWNIK" in l or "MENEDŻER" in l:
            levels[index] = "LEAD"
    levels = sorted(list(set(levels)))
    levels.append(levels.pop(1))
    levels.insert(0, "ANY")
    dict_levels = []
    for level in levels:
        dict_levels.append({"label": level.capitalize(), "value": level})
    levels = dict_levels
    ###

    ### Filtracja czesc dalsza
    bucked_data = {level["value"]: [] for level in levels}
    bucked_data["ALL"] = []
    for entry in raw_data:
        level = entry["experience_level"]

        if isinstance(level, list):
            l = ", ".join(level).upper()
        else:
            l = level.upper()

        if "MID" in l or "MEDIUM" in l:
            bucked_data["MID"].append(entry)

        if (
            "JUNIOR" in l
            or "TRAINEE" in l
            or "STAŻYSTA" in l
            or "PRAKTYKANT" in l
            or "ASYSTENT" in l
        ):
            bucked_data["JUNIOR"].append(entry)

        if "SENIOR" in l or "EXPERT" in l or "EKSPERT" in l:
            bucked_data["SENIOR"].append(entry)

        if "PREZES" in l or "DYREKTOR" in l or "KIEROWNIK" in l or "MENEDŻER" in l:
            bucked_data["LEAD"].append(entry)

        bucked_data["ANY"].append(entry)

    data = bucked_data["ANY"]
    ###

    # Upload
    upload_cv = html.Div(
        [
            dcc.Upload(
                id="upload-data",
                children=html.Div(
                    [
                        html.Strong(
                            "Załaduj umiejętności używając swojego CV",
                            style={"color": "red"},
                            id="loading-text",
                        )
                    ]
                ),
                style={
                    "width": "50%",
                    "height": "60px",
                    "lineHeight": "60px",
                    "borderWidth": "1px",
                    "borderStyle": "dashed",
                    "borderRadius": "5px",
                    "textAlign": "center",
                    "margin": "10px",
                },
            )
        ]
    )

    # App layout
    new_line = "\n"
    app.layout = html.Div(
        [
            html.H1(
                "System rekomendacji ofert pracy w IT",
                className="alert alert-primary p-2 mb-2 text-center",
                style={"margin-top": "-5px"},
            ),
            dbc.Accordion(
                [
                    dbc.AccordionItem(
                        [
                            dbc.RadioItems(
                                levels,
                                "ANY",
                                inputStyle={"margin-right": "15px"},
                                id="seniorities",
                                className="mb-4",
                                labelCheckedClassName="text-primary",
                                inputCheckedClassName="border border-primary bg-primary",
                            ),
                        ],
                        title="Doświadczenie zawodowe (Seniority)",
                    ),
                    dbc.AccordionItem([upload_cv], title="Załaduj swoje CV"),
                    dbc.AccordionItem(
                        [
                            tag_dropdown,
                            html.Br(),
                            html.Div(
                                id="value-container", children=[], className="mb-4"
                            ),
                        ],
                        title="Sprecyzuj umiejętności kandydata",
                    ),
                ]
            ),
            html.Br(),
            html.Div(
                [
                    dbc.Button(
                        id="submit-button",
                        n_clicks=0,
                        children="Stwórz rekomendację",
                        color="primary",
                        className="me-1",
                    )
                ],
                style={"text-align": "center", "margin-bottom": "10px"},
            ),
            html.Br(),
            dcc.Loading(
                id="loading",
                type="circle",
                children=html.Div(id="table-container", className="table table-dark"),
            ),
            dcc.Interval(id="progress-interval", interval=500, n_intervals=0),
            store,
            html.Div(id="seniorities-output"),
        ]
    )

    app.run(debug=True, dev_tools_prune_errors=False)
