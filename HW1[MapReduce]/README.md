# Задача 1 (111) 
Посчитайте число вхождений имён собственных длиной от 6 до 9 символов. Имя собственное - это слово, начинающееся с заглавной буквы и последующих маленьких букв (и только маленьких) и которое ни разу не встретилось в тексте с маленькой буквы. При разборе статей очищайте слова от от знаков пунктуации. Результат приведите к нижнему регистру и отсортируйте по убыванию числа вхождений, в случае равенства - лексикографически.

Входные данные: википедия.

Формат вывода в HDFS: имя количество

Вывод на печать: топ10 имен.

Пример вывода:

```
english 8358
states  7264
british 6829
...
```


# Задание 2
Исходные данные
Путь к данным в HDFS: /data/minecraft-server-logs	
```Данные представляют собой логи сервера игры Minecraft, собранные с 29.11.17, 16:55 по 31.12.17, 23:53. Строчка логов имеет вид:
[YYYY-MM-dd.HH:mm:ss] [Thread name/SEVERITY]: Message

Например:
[2017-11-29.17:22:26] [Server thread/INFO]: [0;37;22m[[0;36;1mAdministration[0;36;22

Message содержит информацию о событии, произошедшем на игровом сервере. В частности, сообщения могут быть таких типов:
Обычные события:
[2017-11-29.17:22:26] [Server thread/INFO]: Luck20 lost connection: Server closed

Предупреждения:
[2017-12-0216:33:12] [Server thread/WARN]: Plugin `Administration_Panel v1.1.0' uses the space-character (0x20) in its name `Administration Panel' - this is discouraged

Событие, связанное с плагином или модом. Например: 
[2017-11-29.17:22:26] [Server thread/INFO]: [AntiAd] Disabling AntiAd v2.3.4
Видим, что сообщение инициировано плагином [AntiAd].
Ошибка. Отличается тем, что может содержать несколько строк, например:
[2017-11-29.16:55:43] [Main thread/ERROR]: Could not load 'plugins/EssentialsSpawn-2.x-SNAPSHOT.jar' in folder 'plugins'
org.bukkit.plugin.InvalidDescriptionException: Invalid plugin.yml
	at org.bukkit.plugin.java.JavaPluginLoader.getPluginDescription(JavaPluginLoader.java:162) ~[spigot-1.8.8.jar:git-Spigot-db6de12-18fbb24]
	at org.bukkit.plugin.SimplePluginManager.loadPlugins(SimplePluginManager.java:133) [spigot-1.8.8.jar:git-Spigot-db6de12-18fbb24]
	at org.bukkit.craftbukkit.v1_8_R3.CraftServer.loadPlugins(CraftServer.java:292) [spigot-1.8.8.jar:git-Spigot-db6de12-18fbb24]
	at net.minecraft.server.v1_8_R3.DedicatedServer.init(DedicatedServer.java:198) [spigot-1.8.8.jar:git-Spigot-db6de12-18fbb24]
	at net.minecraft.server.v1_8_R3.MinecraftServer.run(MinecraftServer.java:525) [spigot-1.8.8.jar:git-Spigot-db6de12-18fbb24]
	at java.lang.Thread.run(Thread.java:745) [?:1.7.0_85]
```

# Задача 1 [120]. “Мониторинг”
Посчитать кол-во warning’ов и ошибок по дням. Результат отсортировать по кол-ву ошибок. При равном кол-ве ошибок отсортировать по кол-ву предупреждений.

Формат вывода:
`YYYY-MM-dd <tab> errors <tab> warnings`

Пример вывода:
`2017-11-29    20   11
2017-12-02    16   61
`