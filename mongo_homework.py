from pymongo import MongoClient
from datetime import datetime, timedelta
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archived_users = db["archived_users"]

# Присвоим значения дат 14 и 30 дней назад переменным
registration_date_30days_ago = datetime.now() - timedelta(days=30)
last_activity_date_14days_ago = datetime.now() - timedelta(days=14)

# Найдем всех пользователей, которые зарегистрировались 30 дней назад и неактивные более 14 дней
archieved_users_list = list(collection.find(
    {
        "$and": [
            {"user_info.registration_date": {"$lt": registration_date_30days_ago}
             }
            ,
            {"event_time": {"$lt": last_activity_date_14days_ago}}]}
))

if archieved_users_list:
    # скопируем архивных пользователей в коллекцию archived_users
    archived_users.insert_many(archieved_users_list)

    # создадим отдельный список с id архивных пользователей
    archieved_user_ids = list(archived_users.distinct("user_id"))
    # удалим архивных пользователей из коллекции всех пользователей
    collection.delete_many({"user_id": {"$in": archieved_user_ids}})

    # сформируем отчет для последующей передачи в json
    archived_users_report = {
        "date": datetime.now().strftime('%Y-%m-%d'),
        "archieved_users_count": len(archieved_users_list),
        "archieved_user_ids": archieved_user_ids,
    }

    # Запишем данные в JSON файл
    archived_users_report_name = datetime.now().strftime('%Y-%m-%d')
    with open(f'{archived_users_report_name}.json', "w", encoding="utf-8") as f:
        json.dump(archived_users_report, f, indent=2, ensure_ascii=False)
    print(f"Отчет успешно сохранен")

else:
    print("Арххивных пользователей не найдено")
