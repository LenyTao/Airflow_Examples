# Airflow UI

* http://127.0.0.1:8080

# Credentials для входа в UI

* USER: admin
* PASSWORD: admin

# Запуск примеров

### Для запуска дага Second_Example_Dag необходимо
+ Создать connection с именем Spark_Shower_Conn,
где user может быть любой,
(он будет выведен в лог при выполнении таски) 
а пароль просто заглушка может быть введено всё что угодно 
+ Отредактировать дефолтный connection - spark_default, нужно host с yarn поменял на local[*]
+ Спустя несколько секунд даг будет готов к выполнению



