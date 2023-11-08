with
card_transactions as (
select ct.value_day, ct.client_pin, cf.name as cashflowcategory_name, ct.opdate_time, ct.summarur_amt
  from s_dmrb.cardtransaction_stran ct
  left join s_dwh4dm.cardtrncfcategory_stran cfc
    on ct.transno_ncode = cfc.transno_ncode
   and cfc.deleted_flag = 'N'
   and cfc.date_part <= cast(date_format('{{ date }}', 'YYYYMMdd') as int)
   and cfc.date_part >= cast(date_format(date_add('{{ date }}', -365), 'YYYYMMdd') as int)
  left join s_dwh4dm.cashflowcategory_ldim cf
    on cfc.cashflowcategory_dk = cf.dk
   and cf.deleted_flag = 'N'
 where ct.deleted_flag = 'N'
   and ct.client_uk > 0
   and ct.summarur_amt > 0
   and ct.date_part < cast(date_format(to_date('{{ date }}'), 'yyyyMMdd') as int)
   and ct.date_part > cast(date_format(date_add(to_date('{{ date }}'), -365), 'yyyyMMdd') as int)
),

clients as (
SELECT client_pin, 
       max(start_date) as start_date
  FROM l_deriveddata.client
 WHERE client_pin NOT IN ('??????')
   AND start_date IS NOT NULL
 GROUP BY client_pin
),

agg as (
SELECT ct.client_pin,
       ct.cashflowcategory_name,
       clients.start_date,
       count(*) / (datediff('{{ date }}', greatest(to_date(clients.start_date), date_add('{{ date }}', -365))) / 30) AS cnt,
       sum(summarur_amt) / (datediff('{{ date }}', greatest(to_date(clients.start_date), date_add('{{ date }}', -365))) / 30) AS amount
  FROM card_transactions ct
  JOIN clients 
    ON ct.client_pin = clients.client_pin
 GROUP BY ct.client_pin, ct.cashflowcategory_name, clients.start_date
)

SELECT
client_pin AS client_pin,
sum(CASE WHEN cashflowcategory_name = 'ПРОДУКТЫ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__produkty,
sum(CASE WHEN cashflowcategory_name = 'ВЫДАЧА НАЛИЧНЫХ В БАНКОМАТЕ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__vydacha_nalichnyh_v_bankomate,
sum(CASE WHEN cashflowcategory_name = 'СУПЕРМАРКЕТЫ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__supermarkety,
sum(CASE WHEN cashflowcategory_name = 'ГИПЕРМАРКЕТЫ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__gipermarkety,
sum(CASE WHEN cashflowcategory_name = 'КАФЕ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kafe,
sum(CASE WHEN cashflowcategory_name = 'ТАКСИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__taksi,
sum(CASE WHEN cashflowcategory_name = 'АЛКОГОЛЬ' THEN amount ELSE NULL END)
AS avg_by_category__amount__sum__cashflowcategory_name__alkogol, 
sum(CASE WHEN cashflowcategory_name = 'ПЕРЕВОДЫ С КАРТЫ НА КАРТУ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__perevody_s_karty_na_kartu,
sum(CASE WHEN cashflowcategory_name = 'АВТОЗАПРАВКИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__avtozapravki,
sum(CASE WHEN cashflowcategory_name = 'АПТЕКИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__apteki,
sum(CASE WHEN cashflowcategory_name = 'ЭЛЕКТРОННЫЕ ДЕНЬГИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__elektronnye_dengi,
sum(CASE WHEN cashflowcategory_name = 'ОДЕЖДА' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__odezhda,
sum(CASE WHEN cashflowcategory_name = 'ФАСТФУД' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__fastfud,
sum(CASE WHEN cashflowcategory_name = 'ВСЁ ДЛЯ ДОМА' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__vse_dlja_doma,
sum(CASE WHEN cashflowcategory_name = 'АВТОМОБИЛЬ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__avtomobil,
sum(CASE WHEN cashflowcategory_name = 'ЗАРУБЕЖНЫЕ ФИНАНСОВЫЕ ОПЕРАЦИИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__zarubezhnye_finansovye_operatsii,
sum(CASE WHEN cashflowcategory_name = 'КОСМЕТИКА' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kosmetika,
sum(CASE WHEN cashflowcategory_name = 'ТОВАРЫ И УСЛУГИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__tovary_i_uslugi,
sum(CASE WHEN cashflowcategory_name = 'ДОМ, КВАРТИРА' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__dom_kvartira,
sum(CASE WHEN cashflowcategory_name = 'ТРАНСПОРТ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__transport,
sum(CASE WHEN cashflowcategory_name = 'ОНЛАЙН МУЗЫКА' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__onlajn_muzyka,
sum(CASE WHEN cashflowcategory_name = 'CОТОВАЯ СВЯЗЬ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__cotovaja_svjaz,
sum(CASE WHEN cashflowcategory_name = 'РЕМОНТ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__remont,
sum(CASE WHEN cashflowcategory_name = 'МЕТРО' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__metro,
sum(CASE WHEN cashflowcategory_name = 'МАРКЕТПЛЕЙСЫ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__marketplejsy,
sum(CASE WHEN cashflowcategory_name = 'СПОРТИВНАЯ ОДЕЖДА' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sportivnaja_odezhda,
sum(CASE WHEN cashflowcategory_name = 'РАЗВЛЕЧЕНИЯ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__razvlechenija,
sum(CASE WHEN cashflowcategory_name = 'СЕТЬ СУПЕРМАРКЕТОВ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__set_supermarketov,
sum(CASE WHEN cashflowcategory_name = 'ТОВАРЫ ДЛЯ ДЕТЕЙ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__tovary_dlja_detej,
sum(CASE WHEN cashflowcategory_name = 'ХОББИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__hobbi,
sum(CASE WHEN cashflowcategory_name = 'КИНО' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kino,
sum(CASE WHEN cashflowcategory_name = 'ГЕМБЛИНГ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__gembling,
sum(CASE WHEN cashflowcategory_name = 'ЗООМАГАЗИНЫ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__zoomagaziny,
sum(CASE WHEN cashflowcategory_name = 'БАРЫ И ПАБЫ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__bary_i_paby,
sum(CASE WHEN cashflowcategory_name = 'КАФЕ-ПЕКАРНИ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kafe_pekarni,
sum(CASE WHEN cashflowcategory_name = 'ОБСЛУЖИВАНИЕ КОМПЬЮТЕРОВ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__obsluzhivanie_kompjuterov,
sum(CASE WHEN cashflowcategory_name = 'МЕДИЦИНСКИЕ ЦЕНТРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__meditsinskie_tsentry,
sum(CASE WHEN cashflowcategory_name = 'КОФЕЙНИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kofejni,
sum(CASE WHEN cashflowcategory_name = 'ЭЛЕКТРИЧКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__elektrichki,
sum(CASE WHEN cashflowcategory_name = 'ЦВЕТЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__tsvety,
sum(CASE WHEN cashflowcategory_name = 'ИНТЕРНЕТ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__internet,
sum(CASE WHEN cashflowcategory_name = 'ТОРГОВЫЕ ЦЕНТРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__torgovye_tsentry,
sum(CASE WHEN cashflowcategory_name = 'ОТЕЛИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__oteli,
sum(CASE WHEN cashflowcategory_name = 'КОММУНАЛЬНЫЕ УСЛУГИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kommunalnye_uslugi,
sum(CASE WHEN cashflowcategory_name = 'АВТОЗАПЧАСТИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__avtozapchasti,
sum(CASE WHEN cashflowcategory_name = 'ПАРКОВКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__parkovki,
sum(CASE WHEN cashflowcategory_name = 'ДЕТСКАЯ ОДЕЖДА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__detskaja_odezhda,
sum(CASE WHEN cashflowcategory_name = 'РЕКЛАМА В ИНТЕРНЕТЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__reklama_v_internete,
sum(CASE WHEN cashflowcategory_name = 'ПОКУПКА ОДЕЖДЫ ОНЛАЙН'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__pokupka_odezhdy_onlajn,
sum(CASE WHEN cashflowcategory_name = 'НАЛОГИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__nalogi,
sum(CASE WHEN cashflowcategory_name = 'АВТОМОЙКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__avtomojki,
sum(CASE WHEN cashflowcategory_name = 'ПОКУПКА ИГР'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__pokupka_igr,
sum(CASE WHEN cashflowcategory_name = 'АВИАКОМПАНИИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__aviakompanii,
sum(CASE WHEN cashflowcategory_name = 'ЗДОРОВЬЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__zdorove,
sum(CASE WHEN cashflowcategory_name = 'КНИГИ И ПРОЧАЯ ПЕЧАТНАЯ ПРОДУКЦИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__knigi_i_prochaja_pechatnaja_produktsija,
sum(CASE WHEN cashflowcategory_name = 'ПОКУПКИ ВНУТРИ ИГР'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__pokupki_vnutri_igr,
sum(CASE WHEN cashflowcategory_name = 'БИЛЕТЫ НА ПОЕЗДА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__bilety_na_poezda,
sum(CASE WHEN cashflowcategory_name = 'ПУТЕШЕСТВИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__puteshestvija,
sum(CASE WHEN cashflowcategory_name = 'ОНЛАЙН КИНОТЕАТРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__onlajn_kinoteatry,
sum(CASE WHEN cashflowcategory_name = 'КАРШЕРИНГ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__karshering,
sum(CASE WHEN cashflowcategory_name = 'КНИГИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__knigi,
sum(CASE WHEN cashflowcategory_name = 'САЛОНЫ КРАСОТЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__salony_krasoty,
sum(CASE WHEN cashflowcategory_name = 'СОФТ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__soft,
sum(CASE WHEN cashflowcategory_name = 'СУШИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sushi,
sum(CASE WHEN cashflowcategory_name = 'ИНТЕРНЕТ СУПЕРМАРКЕТ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__internet_supermarket,
sum(CASE WHEN cashflowcategory_name = 'ПИЦЦА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__pitstsa,
sum(CASE WHEN cashflowcategory_name = 'НИЖНЕЕ БЕЛЬЁ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__nizhnee_bele,
sum(CASE WHEN cashflowcategory_name = 'ПЛАТНЫЕ ДОРОГИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__platnye_dorogi,
sum(CASE WHEN cashflowcategory_name = 'ПЛАТЕЖИ ЧЕРЕЗ ИНТЕРНЕТ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__platezhi_cherez_internet,
sum(CASE WHEN cashflowcategory_name = 'БЫТОВАЯ ТЕХНИКА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__bytovaja_tehnika,
sum(CASE WHEN cashflowcategory_name = 'БИЛЕТЫ НА КОНЦЕРТЫ И В ТЕАТРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__bilety_na_kontserty_i_v_teatry,
sum(CASE WHEN cashflowcategory_name = 'БРОКЕРСКИЕ УСЛУГИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__brokerskie_uslugi,
sum(CASE WHEN cashflowcategory_name = 'ЮВЕЛИРНЫЕ УКРАШЕНИЯ'  THEN amount ELSE NULL END)
AS avg_by_category__amount__sum__cashflowcategory_name__juvelirnye_ukrashenija,
sum(CASE WHEN cashflowcategory_name = 'ТАБАК'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__tabak,
sum(CASE WHEN cashflowcategory_name = 'ОНЛАЙН БИЛЕТЫ В КИНО'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__onlajn_bilety_v_kino,
sum(CASE WHEN cashflowcategory_name = 'АКСЕССУАРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__aksessuary,
sum(CASE WHEN cashflowcategory_name = 'СТОМАТОЛОГИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__stomatologija,
sum(CASE WHEN cashflowcategory_name = 'ОБРАЗОВАНИЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__obrazovanie,
sum(CASE WHEN cashflowcategory_name = 'DUTY FREE'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__duty_free,
sum(CASE WHEN cashflowcategory_name = 'КАНЦТОВАРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kantstovary,
sum(CASE WHEN cashflowcategory_name = 'ИНТЕРНЕТ МАГАЗИНЫ КНИГ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__internet_magaziny_knig,
sum(CASE WHEN cashflowcategory_name = 'БЛАГОТВОРИТЕЛЬНОСТЬ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__blagotvoritelnost,
sum(CASE WHEN cashflowcategory_name = 'ПЛАТЕЖИ ЧЕРЕЗ ТЕРМИНАЛЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__platezhi_cherez_terminaly,
sum(CASE WHEN cashflowcategory_name = 'ДОСТАВКА ЕДЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__dostavka_edy,
sum(CASE WHEN cashflowcategory_name = 'АРЕНДА АВТОМОБИЛЕЙ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__arenda_avtomobilej,
sum(CASE WHEN cashflowcategory_name = 'ОБСЛУЖИВАНИЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__obsluzhivanie,
sum(CASE WHEN cashflowcategory_name = 'ФОТОПЕЧАТЬ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__fotopechat,
sum(CASE WHEN cashflowcategory_name = 'СТРАХОВАНИЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__strahovanie,
sum(CASE WHEN cashflowcategory_name = 'КУПОНЫ И СКИДКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kupony_i_skidki,
sum(CASE WHEN cashflowcategory_name = 'СПУТНИКОВОЕ ТВ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sputnikovoe_tv,
sum(CASE WHEN cashflowcategory_name = 'БУРГЕРНЫЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__burgernye,
sum(CASE WHEN cashflowcategory_name = 'ФИНАНСЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__finansy,
sum(CASE WHEN cashflowcategory_name = 'БИЛЕТЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__bilety,
sum(CASE WHEN cashflowcategory_name = 'МАГАЗИНЫ ПОДАРКОВ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__magaziny_podarkov,
sum(CASE WHEN cashflowcategory_name = 'АВТОБУСЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__avtobusy,
sum(CASE WHEN cashflowcategory_name = 'ЗНАКОМСТВА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__znakomstva,
sum(CASE WHEN cashflowcategory_name = 'ФИТНЕС'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__fitnes,
sum(CASE WHEN cashflowcategory_name = 'ОПТИКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__optiki,
sum(CASE WHEN cashflowcategory_name = 'ДЕТСКИЕ ИГРУШКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__detskie_igrushki,
sum(CASE WHEN cashflowcategory_name = 'ИНВЕСТИЦИИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__investitsii,
sum(CASE WHEN cashflowcategory_name = 'БАРБЕРШОПЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__barbershopy,
sum(CASE WHEN cashflowcategory_name = 'ЛОГИСТИКА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__logistika,
sum(CASE WHEN cashflowcategory_name = 'АГРЕГАТОРЫ ДЛЯ ПУТЕШЕСТВЕННИКОВ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__agregatory_dlja_puteshestvennikov,
sum(CASE WHEN cashflowcategory_name = 'ШТРАФЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__shtrafy,
sum(CASE WHEN cashflowcategory_name = 'ОБЛАЧНЫЕ ХРАНИЛИЩА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__oblachnye_hranilischa,
sum(CASE WHEN cashflowcategory_name = 'АВТОСАЛОНЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__avtosalony,
sum(CASE WHEN cashflowcategory_name = 'БАРЫ В КИНОТЕАТРАХ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__bary_v_kinoteatrah,
sum(CASE WHEN cashflowcategory_name = 'РЕЛИГИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__religija,
sum(CASE WHEN cashflowcategory_name = 'АКВАПАРКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__akvaparki,
sum(CASE WHEN cashflowcategory_name = 'КЛИНИКИ ДЛЯ БЕРЕМЕННЫХ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kliniki_dlja_beremennyh,
sum(CASE WHEN cashflowcategory_name = 'БИНАРНЫЕ ОПЦИОНЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__binarnye_optsiony,
sum(CASE WHEN cashflowcategory_name = 'АНАЛИЗЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__analizy,
sum(CASE WHEN cashflowcategory_name = 'МАНИКЮР И ПЕДИКЮР'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__manikjur_i_pedikjur,
sum(CASE WHEN cashflowcategory_name = 'СПОРТИВНЫЕ РАЗВЛЕЧЕНИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sportivnye_razvlechenija,
sum(CASE WHEN cashflowcategory_name = 'РЕСТОРАНЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__restorany,
sum(CASE WHEN cashflowcategory_name = 'ПОЛИКЛИНИКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__polikliniki,
sum(CASE WHEN cashflowcategory_name = 'ХОСТИНГ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__hosting,
sum(CASE WHEN cashflowcategory_name = 'ПАРКИ ОТДЫХА И РАЗВЛЕЧЕНИЙ'  THEN amount ELSE NULL END)
AS avg_by_category__amount__sum__cashflowcategory_name__parki_otdyha_i_razvlechenij,
sum(CASE WHEN cashflowcategory_name = 'МЯСНАЯ ПРОДУКЦИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__mjasnaja_produktsija,
sum(CASE WHEN cashflowcategory_name = 'ЧАСЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__chasy,
sum(CASE WHEN cashflowcategory_name = 'СПОРТ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sport,
sum(CASE WHEN cashflowcategory_name = 'ПЕРЕВОДЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__perevody,
sum(CASE WHEN cashflowcategory_name = 'СПА, САУНЫ, БАНИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__spa_sauny_bani,
sum(CASE WHEN cashflowcategory_name = 'ИНТЕРНЕТ ТЕЛЕФОНИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__internet_telefonija,
sum(CASE WHEN cashflowcategory_name = 'МАСТЕРКЛАССЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__masterklassy,
sum(CASE WHEN cashflowcategory_name = 'ПРОЧИЕ БИЛЕТЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__prochie_bilety,
sum(CASE WHEN cashflowcategory_name = 'ЛОУКОСТЕРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__loukostery,
sum(CASE WHEN cashflowcategory_name = 'ОКЕАНАРИУМЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__okeanariumy,
sum(CASE WHEN cashflowcategory_name = 'СПЕЦ ОДЕЖДА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__spets_odezhda,
sum(CASE WHEN cashflowcategory_name = 'ОДЕЖДА ДЛЯ БЕРЕМЕННЫХ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__odezhda_dlja_beremennyh,
sum(CASE WHEN cashflowcategory_name = 'ЛЕТСПЛЕЙ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__letsplej,
sum(CASE WHEN cashflowcategory_name = 'ВОЗВРАТ НАЛОГОВ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__vozvrat_nalogov,
sum(CASE WHEN cashflowcategory_name = 'АНТИКВАРИАТ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__antikvariat,
sum(CASE WHEN cashflowcategory_name = 'АВТОЗАПЧАСТИ ДЛЯ ГРУЗОВИКОВ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__avtozapchasti_dlja_gruzovikov,
sum(CASE WHEN cashflowcategory_name = 'АУДИОКНИГИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__audioknigi,
sum(CASE WHEN cashflowcategory_name = 'ВИЗОВЫЕ СБОРЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__vizovye_sbory,
sum(CASE WHEN cashflowcategory_name = 'ТУРИСТИЧЕСКИЕ АГЕНСТВА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__turisticheskie_agenstva,
sum(CASE WHEN cashflowcategory_name = 'БАССЕЙНЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__bassejny,
sum(CASE WHEN cashflowcategory_name = 'БЕГ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__beg,
sum(CASE WHEN cashflowcategory_name = 'ПОЛИТИКА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__politika,
sum(CASE WHEN cashflowcategory_name = 'КРАСОТА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__krasota,
sum(CASE WHEN cashflowcategory_name = 'ПРОЧИЕ РАЗВЛЕЧЕНИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__prochie_razvlechenija,
sum(CASE WHEN cashflowcategory_name = 'СПОРТИВНОЕ ПИТАНИЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sportivnoe_pitanie,
sum(CASE WHEN cashflowcategory_name = 'ГОРНОЛЫЖНЫЕ КУРОРТЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__gornolyzhnye_kurorty,
sum(CASE WHEN cashflowcategory_name = 'ФОРЕКС'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__foreks,
sum(CASE WHEN cashflowcategory_name = 'МФО'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__mfo,
sum(CASE WHEN cashflowcategory_name = 'ИНТЕРНЕТ КОMМЕРЦИЯ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__internet_kommertsija,
sum(CASE WHEN cashflowcategory_name = 'ЦИФРОВАЯ ПЕЧАТЬ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__tsifrovaja_pechat,
sum(CASE WHEN cashflowcategory_name = 'ЗООПАРКИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__zooparki,
sum(CASE WHEN cashflowcategory_name = 'ПРЫЖКИ С ПАРАШЮТОМ '  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__pryzhki_s_parashjutom_,
sum(CASE WHEN cashflowcategory_name = 'ЙОГА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__joga,
sum(CASE WHEN cashflowcategory_name = 'КРАУДФАНДИНГ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kraudfanding,
sum(CASE WHEN cashflowcategory_name = 'SMS-ПЛАТЕЖИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sms_platezhi,
sum(CASE WHEN cashflowcategory_name = 'АВИАБИЛЕТЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__aviabilety,
sum(CASE WHEN cashflowcategory_name = 'ДОСТАВКА КОМПЛЕКСНЫХ ЗАВТРАКОВ, ОБЕДОВ, УЖИНОВ' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__dostavka_kompleksnyh_zavtrakov_obedov_uzhinov,
sum(CASE WHEN cashflowcategory_name = 'КОВОРКИНГИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kovorkingi,
sum(CASE WHEN cashflowcategory_name = 'КРИПТОВАЛЮТЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__kriptovaljuty,
sum(CASE WHEN cashflowcategory_name = 'НОЧНЫЕ КЛУБЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__nochnye_kluby,
sum(CASE WHEN cashflowcategory_name = 'ОНЛАЙН ОБРАЗОВАНИЕ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__onlajn_obrazovanie,
sum(CASE WHEN cashflowcategory_name = 'ОХОТА И РЫБАЛКА'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__ohota_i_rybalka,
sum(CASE WHEN cashflowcategory_name = 'ПАРОМЫ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__paromy,
sum(CASE WHEN cashflowcategory_name = 'ПЛАТЕЖИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__platezhi,
sum(CASE WHEN cashflowcategory_name = 'ПРОКАТ ВЕЛОСИПЕДОВ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__prokat_velosipedov,
sum(CASE WHEN cashflowcategory_name = 'СОЛЯРИИ'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__soljarii,
sum(CASE WHEN cashflowcategory_name = 'СПОРТИВНЫЕ ПРОГРАММЫ/ТРЕНЕРЫ ОНЛАЙН' THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__sportivnye_programmy_trenery_onlajn,
sum(CASE WHEN cashflowcategory_name = 'ТЕННИС'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__tennis,
sum(CASE WHEN cashflowcategory_name = 'ТРИАТЛОН'  THEN amount ELSE NULL END) 
AS avg_by_category__amount__sum__cashflowcategory_name__triatlon,
sum(CASE WHEN cashflowcategory_name = 'ЧАСТНЫЕ САМОЛЁТЫ' THEN amount ELSE NULL END)
AS avg_by_category__amount__sum__cashflowcategory_name__chastnye_samolety,
'{{ date }}' as dt

  FROM agg
 GROUP BY client_pin