import streamlit as st
import sqlite3
import polars as pl
import plotly.express as px


DB_PATH = "data/weather.db"

st.set_page_config(page_title="WeatherInsight", layout="wide")
st.title("🌦️ WeatherInsight: Погодные тренды")

# Загрузка данных
@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pl.read_database("SELECT * FROM weather ORDER BY date", conn)
    conn.close()
    return df

try:
    df = load_data()
except Exception as e:
    st.error("❌ Не удалось загрузить данные. Убедитесь, что база данных существует и доступна.")
    st.stop()

if "primary_city" not in st.session_state:
    st.session_state["primary_city"] = ''
if "secondary_city" not in st.session_state:
    st.session_state["secondary_city"] = ''
if "cities" not in st.session_state:
    st.session_state["cities"] = list()
if "cities_1" not in st.session_state:
    st.session_state["cities_1"] = list()
if "number_page" not in st.session_state:
    st.session_state["number_page"] = 1
if "total_page" not in st.session_state:
    st.session_state["total_page"] = 1
if "disabled_button_1" not in st.session_state:
    st.session_state["disabled_button_1"] = False
if "disabled_button_2" not in st.session_state:
    st.session_state["disabled_button_2"] = False
if "disabled_button_3" not in st.session_state:
    st.session_state["disabled_button_3"] = False
if "disabled_button_4" not in st.session_state:
    st.session_state["disabled_button_4"] = False
if "disabled_button_5" not in st.session_state:
    st.session_state["disabled_button_5"] = False

# Статистика по всем данным
st.subheader("📊 Общая статистика")
total_records = len(df)
unique_cities = df["city"].n_unique()
st.write(f"Всего записей: {total_records}")
st.write(f"Уникальных городов: {unique_cities}")

# Выбор города
st.session_state["cities"] = sorted(df["city"].unique().to_list()) #подготовка списка городов для основного города
                                                                    # selectbox

#подготовка списка городов для сравнимаемого selectbox с учетом удаления выбранного города в первом selectbox
if st.session_state["cities_1"] == []:
    st.session_state["cities_1"] = sorted(df["city"].unique().to_list())
    st.session_state["cities_1"][0] = 'None'

def on_change_primary_city(): #обработка собития смены города и изменени select box  сравнимаемого города
    st.session_state["cities_1"] = st.session_state["cities"]
    st.session_state["cities_1"].remove(st.session_state["primary_city"])
    st.session_state["cities_1"].insert(0, 'None')
    return
# компоненты для работы с городоми и датами
col_1, col_2, col_3, col_4 = st.columns(4)
with col_1:
    primary_city = st.selectbox("Выберите город",
                                st.session_state["cities"],
                                key="primary_city",
                                on_change=on_change_primary_city)
with col_2:
    date_1 = st.date_input("Дата начало статистики",
                           format="DD/MM/YYYY",
                           key='date_1',
                           value='2026-01-01')
with col_3:
    date_2 = st.date_input("Дата окончания статистики",
                           format="DD/MM/YYYY",
                           key='date_2',
                           value='2026-03-31')
with col_4:
    secondary_city = st.selectbox("Выберите город для сравнения",
                                  st.session_state["cities_1"],
                                  key="secondary_city")

st.subheader("Общие данные по выбранным городам")

#подготовка данных,
df = df.fill_null(strategy="mean") # Работат с пропусками

# преобразование в стольба в тип дата
df = df.select(
    (pl.col('city')),
    (pl.col('date').str.split(' ').list.get(0)),
    (pl.col('avg_temp')),
    (pl.col('total_precip')),
    (pl.col('avg_wind')),
    (pl.col('is_rainy'))
)
df = df.select(
    (pl.col('city')),
    (pl.col('date').str.to_date()),
    (pl.col('avg_temp').round(0)),
    (pl.col('total_precip')),
    (pl.col('avg_wind').round(0)),
    (pl.col('is_rainy'))
)

# данные после фильрации выбора по города и датам
city_data_1 = df.filter((pl.col("city") == st.session_state["primary_city"]) &
                        (pl.col("date") >= st.session_state["date_1"]) &
                        (pl.col("date") <= st.session_state["date_2"]))
city_data_2 = df.filter((pl.col("city") == st.session_state["secondary_city"])  &
                        (pl.col("date") >= st.session_state["date_1"]) &
                        (pl.col("date") <= st.session_state["date_2"]))

# условия определянт наличие всего страниц, для коректного постоянно вывода необхомо явно указывать всего страниц 1 при
# наличие менее 10 записей
if 10 < len(city_data_1):
    st.session_state["total_page"] = len(city_data_1) // 10 + 1
else:
    st.session_state["total_page"] = 1

# Условия проверки на активнойсть кнопок
# Условия для первой странице
if st.session_state["number_page"] == 1:
    st.session_state["disabled_button_1"] = True
    st.session_state["disabled_button_2"] = True
else:
    st.session_state["disabled_button_1"] = False
    st.session_state["disabled_button_2"] = False

# Условия для последней странице
if st.session_state["number_page"] == st.session_state["total_page"]:
    st.session_state["disabled_button_3"] = True
    st.session_state["disabled_button_4"] = True
else:
    st.session_state["disabled_button_3"] = False
    st.session_state["disabled_button_4"] = False

# Деактивация кнопок если записей меньше 10
if 11 > len(city_data_1):
    st.session_state["number_page"] = 1
    st.session_state["disabled_button_1"] = True
    st.session_state["disabled_button_2"] = True
    st.session_state["disabled_button_3"] = True
    st.session_state["disabled_button_4"] = True

# функции отработки кликов кнопок листания и обработка переменной номер страница
def click_button_1():
    st.session_state["number_page"] = 1
def click_button_2():
    st.session_state["number_page"] = st.session_state["number_page"] - 1
def click_button_3():
    st.session_state["number_page"] = st.session_state["number_page"] + 1
def click_button_4():
    st.session_state["number_page"] = len(city_data_1) // 10 + 1

# Вывод кнопок для листания данных
col1_1, col1_2, col1_3, col1_4, col1_5, col1_6, col1_7, col1_8 = st.columns(8)
with col1_3:
    is_pressed_1 = st.button("В начало",
                             key='button_1',
                             disabled=st.session_state["disabled_button_1"],
                             on_click=click_button_1)
with col1_4:
    is_pressed_2 = st.button("Назад",
                             key='button_2',
                             disabled=st.session_state["disabled_button_2"],
                             on_click=click_button_2)
with col1_5:
    is_pressed_3 = st.button("Вперед",
                             key='button_3',
                             disabled=st.session_state["disabled_button_3"],
                             on_click=click_button_3)
with col1_6:
    is_pressed_4 = st.button("В конец",
                             key='button_4',
                             disabled=st.session_state["disabled_button_4"],
                             on_click=click_button_4)
with col1_2:
    st.write(f"Всего: {st.session_state["total_page"]}")

# повторяеться, для корректной работы отработки тригера нажатия
# Условия проверки на активнойсть кнопок

# Условия для первой странице
if st.session_state["number_page"] == 1:
    st.session_state["disabled_button_1"] = True
    st.session_state["disabled_button_2"] = True
else:
    st.session_state["disabled_button_1"] = False
    st.session_state["disabled_button_2"] = False

# Условия для последней странице
if st.session_state["number_page"] == st.session_state["total_page"]:
    st.session_state["disabled_button_3"] = True
    st.session_state["disabled_button_4"] = True
else:
    st.session_state["disabled_button_3"] = False
    st.session_state["disabled_button_4"] = False

# Деактивация кнопок если записей меньше 11
if 11 > len(city_data_1):
    st.session_state["number_page"] = 1
    st.session_state["disabled_button_1"] = True
    st.session_state["disabled_button_2"] = True
    st.session_state["disabled_button_3"] = True
    st.session_state["disabled_button_4"] = True

with col1_1:
    st.write(f"Страница: {st.session_state["number_page"]}")

#Временные талблицы для вывода на экран с листанием

if 11 > len(city_data_1):
    city_data_1_tmp = city_data_1
    if not city_data_2.is_empty():
        city_data_2_tmp = city_data_2
else:
    if st.session_state["number_page"] == st.session_state["total_page"]:
        city_data_1_tmp = city_data_1.tail(len(city_data_1) % 10)
        if not city_data_2.is_empty():
            city_data_2_tmp = city_data_2.tail(len(city_data_2) % 10)

    else:
        city_data_1_tmp = city_data_1.head(st.session_state["number_page"] * 10)
        city_data_1_tmp = city_data_1_tmp.tail(10)
        if not city_data_2.is_empty():
            city_data_2_tmp = city_data_2.head(st.session_state["number_page"] * 10)
            city_data_2_tmp = city_data_2_tmp.tail(10)

graf_1, graf_2 = st.columns(2) # колонки для вывода графиков сравнения

with graf_1:
    st.dataframe(city_data_1_tmp)
    if not city_data_1.is_empty():
        fig_temp_hist = px.line(
            city_data_1_tmp.to_pandas(),
            x="date",
            y="avg_temp",
            title=f"Средняя температура в {st.session_state["primary_city"]} (За выбранный период, по странично)",
            labels={"avg_temp": "Температура (°C)", "date": "Дата"}
        )
        st.plotly_chart(fig_temp_hist, width='stretch') # For `use_container_width=True`, use `width='stretch'`. For `use_container_width=False`, use `width='content'`.

with graf_2:
    if st.session_state["secondary_city"] != 'None':
        st.dataframe(city_data_2_tmp)
        if not city_data_2.is_empty():
            fig_temp_hist = px.line(
                city_data_2_tmp.to_pandas(),
                x="date",
                y="avg_temp",
                title=f"Средняя температура в {st.session_state["secondary_city"]} (За выбранный период, по странично)",
                labels={"avg_temp": "Температура (°C)", "date": "Дата"}
            )
            st.plotly_chart(fig_temp_hist, width='stretch')

# Подготовка данных, присвоение градаций значениям
city_data_1_tmp_1 = city_data_1.with_columns(
    (
      pl.when(pl.col("is_rainy") == 1)
     .then(pl.lit('Дождь'))
     .otherwise(pl.lit('Нет дождя'))
     .alias("is_rainy_word")
    ),
    (
     pl.when(pl.col("avg_temp") < 12)
     .then(pl.lit('Холодно'))
     .otherwise(pl.when((pl.col("avg_temp") >= 12) & (pl.col("avg_temp") <= 17))
                 .then(pl.lit('Умеренно'))
                 .otherwise(pl.when(pl.col("avg_temp") > 17)
                            .then(pl.lit('Жарко'))
                            )
                )
     .alias("avg_temp_word_1")
    ),
    (
        pl.when(pl.col("avg_wind") < 5.0)
        .then(pl.lit('Слабый ветер'))
        .otherwise(pl.when((pl.col("avg_wind") >= 5.0) & (pl.col("avg_wind") <= 15.0))
                   .then(pl.lit('Умеренный ветер'))
                   .otherwise(pl.when(pl.col("avg_wind") > 15.0)
                              .then(pl.lit('Шторм'))
                              )
                   )
        .alias("avg_wind_word_1")
    ),
    (
        pl.when(pl.col("total_precip") == 0.0)
        .then(pl.lit('Без осадков'))
        .otherwise(pl.when((pl.col("total_precip") >= 0.0) & (pl.col("total_precip") <= 6.0))
                   .then(pl.lit('Слабый дождь'))
                   .otherwise(pl.when((pl.col("total_precip") > 6.0) & (pl.col("total_precip") < 14.0))
                              .then(pl.lit('Сильный дождь'))
                              .otherwise(pl.when(pl.col("total_precip") > 14.0)
                                        .then(pl.lit('Ливень'))
                                        )
                              )
                   )
        .alias("total_precip_word_1")
    )
)

city_data_2_tmp_1 = city_data_2.with_columns(
    (
      pl.when(pl.col("is_rainy") == 1)
     .then(pl.lit('Дождь'))
     .otherwise(pl.lit('Нет дождя'))
     .alias("is_rainy_word")
    ),
    (
     pl.when(pl.col("avg_temp") < 12)
     .then(pl.lit('Холодно'))
     .otherwise(pl.when((pl.col("avg_temp") >= 12) & (pl.col("avg_temp") <= 17))
                 .then(pl.lit('Умеренно'))
                 .otherwise(pl.when(pl.col("avg_temp") > 17)
                            .then(pl.lit('Жарко'))
                            )
                )
     .alias("avg_temp_word_1")
    ),
    (
        pl.when(pl.col("avg_wind") < 5.0)
        .then(pl.lit('Слабый ветер'))
        .otherwise(pl.when((pl.col("avg_wind") >= 5.0) & (pl.col("avg_wind") <= 15.0))
                   .then(pl.lit('Умеренный ветер'))
                   .otherwise(pl.when(pl.col("avg_wind") > 15.0)
                              .then(pl.lit('Шторм'))
                              )
                   )
        .alias("avg_wind_word_1")
    ),
    (
        pl.when(pl.col("total_precip") == 0.0)
        .then(pl.lit('Без осадков'))
        .otherwise(pl.when((pl.col("total_precip") >= 0.0) & (pl.col("total_precip") <= 6.0))
                   .then(pl.lit('Слабый дождь'))
                   .otherwise(pl.when((pl.col("total_precip") > 6.0) & (pl.col("total_precip") < 14.0))
                              .then(pl.lit('Сильный дождь'))
                              .otherwise(pl.when(pl.col("total_precip") > 14.0)
                                        .then(pl.lit('Ливень'))
                                        )
                              )
                   )
        .alias("total_precip_word_1")
    )
)
# подготовка данных для круговых диаграм дождя
tmp_1 = city_data_1_tmp_1.lazy()
polar_tmp_1 = (
    tmp_1
    .group_by('is_rainy_word')
    .agg(pl.count('is_rainy_word').alias('Total'))
    .collect()
)

with graf_1:
    fig = px.pie(
        polar_tmp_1,
        values='Total',
        names="is_rainy_word",
        width=500,
        height=500,
        title=f"Количество дождливых дней в {st.session_state["primary_city"]}"
    )
    st.plotly_chart(fig, width='stretch')
    st.dataframe(polar_tmp_1, width='content')

tmp_2 = city_data_2_tmp_1.lazy()
polar_tmp_2 = (
    tmp_2
    .group_by('is_rainy_word')
    .agg(pl.count('is_rainy_word').alias('Total'))
    .collect()
)

with graf_2:
    if st.session_state["secondary_city"] != 'None':
        fig = px.pie(
            polar_tmp_2,
            values='Total',
            names="is_rainy_word",
            width=500, height=500,
            title=f"Количество дождливых дней в {st.session_state["secondary_city"]}"
        )
        st.plotly_chart(fig, width='stretch')
        st.dataframe(polar_tmp_2, width='content')

# Объединения данных по 2 городая, для совместного вывода на одном графике
city_data_all = pl.concat([city_data_1_tmp_1, city_data_2_tmp_1])

fig = px.line(
    city_data_all,
    x='date',
    y='avg_temp',
    color='city',
    title=f"Средняя суточная температура в выбранных городах",
    labels={"avg_temp": "Средняя суточная температура (°C)", "date": "Дата", "city": "Город"}
)
st.plotly_chart(fig, width='stretch')

#деление экруна на 3 и части и вывод графиков по созданным градациям
graf_3, graf_4, graf_5 = st.columns(3)

tmp_2 = city_data_all.lazy()
polar_tmp_3 = (
    tmp_2
    .group_by(['city', 'avg_temp_word_1'])
    .agg(pl.count('avg_temp_word_1').alias('Total'))
    .sort(['city','avg_temp_word_1'])
    .collect()
)

with graf_3:
    fig = px.bar(
        polar_tmp_3,
        x='city',
        y='Total',
        color='avg_temp_word_1',
        barmode='group',
        title=f"Количество дней по градации средней суточной температуры",
        labels={"Total": "Количество дней", "city": "Город", "avg_temp_word_1": "Градация:",}
    )
    st.plotly_chart(fig, width='stretch')

tmp_2 = city_data_all.lazy()
polar_tmp_4 = (
    tmp_2
    .group_by(['city', 'avg_wind_word_1'])
    .agg(pl.count('avg_wind_word_1').alias('Total'))
    .sort(['city', 'avg_wind_word_1'])
    .collect()
)

with graf_4:
    fig = px.bar(
        polar_tmp_4,
        x='city',
        y='Total',
        color='avg_wind_word_1',
        barmode='group',
        title=f"Количество дней по градации средне суточным порыва ветра",
        labels={"Total": "Количество дней", "city": "Город", "avg_wind_word_1": "Градация:",}
    )
    st.plotly_chart(fig, width='stretch')

st.write("всего записей", len(polar_tmp_4))

tmp_2 = city_data_all.lazy()
polar_tmp_5 = (
    tmp_2
    .group_by(['city', 'total_precip_word_1'])
    .agg(pl.count('total_precip_word_1').alias('Total'))
    .sort(['city', 'total_precip_word_1'])
    .collect()
)

with graf_5:
    fig = px.bar(
        polar_tmp_5,
        x='city',
        y='Total',
        color='total_precip_word_1',
        barmode='group',
        title=f"Количество дней по градации суточные осадки ",
        labels={"Total": "Количество дней", "city": "Город", "total_precip_word_1": "Градация:",}
    )
    st.plotly_chart(fig, width='stretch')

# Поиск аномальных дней
typhoon_days_primary_city = city_data_1_tmp_1.filter((pl.col("total_precip_word_1") == "Ливень") & #тайфун
                                                     (pl.col("avg_temp_word_1") == "Жарко") &
                                                     (pl.col("avg_wind_word_1") == "Шторм"))

blizzard_days_primary_city = city_data_1_tmp_1.filter((pl.col("total_precip_word_1") == "Ливень") & #метель
                                                      (pl.col("avg_temp_word_1") == "Холодно") &
                                                      (pl.col("avg_wind_word_1") == "Шторм"))
with graf_1:
    st.subheader("⚠️ Аномалии")
    st.write(f"Количество дней с тайфунов в {st.session_state["primary_city"]}: {len(typhoon_days_primary_city)}")
    st.write(f"Количество дней с метелеми в {st.session_state["primary_city"]}: {len(blizzard_days_primary_city)}")


typhoon_days_secondary_city = city_data_2_tmp_1.filter((pl.col("total_precip_word_1") == "Ливень") & #тайфун
                                                     (pl.col("avg_temp_word_1") == "Жарко") &
                                                     (pl.col("avg_wind_word_1") == "Шторм"))

blizzard_days_secondary_city = city_data_2_tmp_1.filter((pl.col("total_precip_word_1") == "Ливень") & #метель
                                                        (pl.col("avg_temp_word_1") == "Холодно") &
                                                        (pl.col("avg_wind_word_1") == "Шторм"))
with graf_2:
    if st.session_state["secondary_city"] != 'None':
        st.subheader("⚠️ Аномалии")
        st.write(f"Количество дней с тайфунов в {st.session_state["secondary_city"]}: "
                 f"{len(typhoon_days_secondary_city)}")
        st.write(f"Количество дней с метелеми в {st.session_state["secondary_city"]}: "
                 f"{len(blizzard_days_secondary_city)}")

st.subheader("Выберите комфортные для себя параметры погоды")
# Кнопка влючеться только при внесение изменений
def click_button_5():
    st.session_state["disabled_button_5"] = True

def cange_button_5():
    st.session_state["disabled_button_5"] = False
#Дружественный интерфейс для пользователя для ввода желаемой погоды в выбранных городах
graf_6, graf_7, graf_8 = st.columns(3)

with graf_6:
    user_avg_temp = st.slider("Среднесуточная температура",
                              -40,
                              +45,
                              [15, 25],
                              1,
                              on_change=cange_button_5)

with graf_7:
    user_avg_wind = st.slider("Среднесуточные порывы ветра ",
                     0,
                              40,
                              [0, 5],
                              1,
                              on_change=cange_button_5)
    is_pressed_5 = st.button("Создать график",
                             key='button_5',
                             disabled=st.session_state["disabled_button_5"],
                             on_click=click_button_5)

with graf_8:
    user_rain = st.radio("Осадки",
                         ["Люблю дождь", "Не люблю дождь"],
                         horizontal=True,
                         on_change=cange_button_5)

if is_pressed_5:     #Применения к постоянной переменной значений для вывода графика
    if user_rain == "Люблю дождь":
        st.session_state["city_data_user_tmp_1"] = city_data_1.with_columns(
            (pl.when(((pl.col("avg_temp") >= user_avg_temp[0]) & (pl.col("avg_temp") <= user_avg_temp[1])) &
                     ((pl.col("avg_wind") >= user_avg_wind[0]) & (pl.col("avg_wind") <= user_avg_wind[1]))
                    )
            .then(pl.lit('Соответсвует требованиям'))
            .otherwise(pl.lit('Не соответсвует требованиям'))
            .alias("is_day_user")
            )
        )
    else:
        st.session_state["city_data_user_tmp_1"] = city_data_1.with_columns(
            (pl.when(((pl.col("avg_temp") >= user_avg_temp[0]) & (pl.col("avg_temp") <= user_avg_temp[1])) &
                     ((pl.col("avg_wind") >= user_avg_wind[0]) & (pl.col("avg_wind") <= user_avg_wind[1])) &
                      (pl.col("is_rainy") == 0)
                    )
                .then(pl.lit('Соответсвует требованиям'))
                .otherwise(pl.lit('Не соответсвует требованиям'))
                .alias("is_day_user")
            )
        )

    if st.session_state["secondary_city"] != 'None':
        if user_rain == "Люблю дождь":
            st.session_state["city_data_user_tmp_2"] = city_data_2.with_columns(
                (pl.when(((pl.col("avg_temp") >= user_avg_temp[0]) & (pl.col("avg_temp") <= user_avg_temp[1])) &
                         ((pl.col("avg_wind") >= user_avg_wind[0]) & (pl.col("avg_wind") <= user_avg_wind[1]))
                         )
                 .then(pl.lit('Соответсвует требованиям'))
                 .otherwise(pl.lit('Не соответсвует требованиям'))
                 .alias("is_day_user")
                 )
            )
        else:
            st.session_state["city_data_user_tmp_2"] = city_data_2.with_columns(
                (pl.when(((pl.col("avg_temp") >= user_avg_temp[0]) & (pl.col("avg_temp") <= user_avg_temp[1])) &
                         ((pl.col("avg_wind") >= user_avg_wind[0]) & (pl.col("avg_wind") <= user_avg_wind[1])) &
                         (pl.col("is_rainy") == 0)
                         )
                 .then(pl.lit('Соответсвует требованиям'))
                 .otherwise(pl.lit('Не соответсвует требованиям'))
                 .alias("is_day_user")
                 )
            )

graf_9, graf_10 = st.columns(2)
#вывод графиков и сразу проверка на наличие первого начатия кнопки, пока не было нажато графики не появяться
if "city_data_user_tmp_1" in st.session_state:
    tmp_5 = st.session_state["city_data_user_tmp_1"].lazy()
    polar_tmp_5 = (
        tmp_5
        .group_by('is_day_user')
        .agg(pl.count('is_day_user').alias('Total'))
        .sort('is_day_user')
        .collect()
    )
    with graf_9:
        fig = px.pie(
            polar_tmp_5,
            values='Total',
            names="is_day_user",
            width=500,
            height=500,
            title=f"Количество подходящих дней в {st.session_state["primary_city"]}"
        )
        st.plotly_chart(fig, width='stretch')
        st.dataframe(polar_tmp_5)

if "city_data_user_tmp_2" in st.session_state:
    if st.session_state["secondary_city"] != 'None':
        tmp_6 = st.session_state["city_data_user_tmp_2"].lazy()
        polar_tmp_6 = (
            tmp_6
            .group_by('is_day_user')
            .agg(pl.count('is_day_user').alias('Total'))
            .sort('is_day_user')
            .collect()
        )
        with graf_10:
            fig = px.pie(
                polar_tmp_6,
                values='Total',
                names="is_day_user",
                width=500,
                height=500,
                title=f"Количество подходящих дней в {st.session_state["secondary_city"]}"
            )
            st.plotly_chart(fig, width='stretch')
            st.dataframe(polar_tmp_6)

#st.dataframe(city_data_all)

#Допольнительное задание

df_daily = city_data_all.with_columns(
    (
        - pl.col("total_precip") * 3          # штраф за осадки (сильнее влияет)
        - pl.col("avg_wind") * 0.7           # штраф за ветер
        - (pl.col("avg_temp") - 20).abs() * 0.6  # идеал ~20°C
    ).alias("comfort_index")
)

fig_comfort = px.line(
    df_daily,
    x="date",
    y="comfort_index",
    color='city',
    title=f"Индекс комфорта в выбранных города за {st.session_state["date_1"]} - {st.session_state["date_2"]} (чем выше — тем комфортнее)",
    labels={"comfort_index": "Индекс комфорта", "date": "Дата"}
)
st.plotly_chart(fig_comfort, width='stretch')


st.dataframe(df_daily)