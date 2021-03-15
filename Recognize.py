
import pytesseract
import cv2
import numpy as np
import sqlite3 as sl
import json
import re
import concurrent.futures as conc
from pyzbar.pyzbar import decode
import tempfile
import os

scale_percent = 50  # percent of original size
delta = 0


def GetText_potok(image, executor,graph,futures, lang, result = None):
    # Макс количество вершин для одного расчета
    max_kol = 10
    spis = []
    for uzel in range(0, len(graph)):
        spis.append(uzel)
        if len(spis) == max_kol:
            future = executor.submit(gettext,image, graph, spis, lang, result)
            futures.append(future)
            # gettext(graph, spis)
            spis = []
    if len(spis) > 0:
        # gettext(graph, spis)
        future = executor.submit(gettext,image, graph, spis, lang, result)
        futures.append(future)

def FillRealParent(cur):

    cur.executescript(""" 
        CREATE TABLE Parent( 
            Index_m,
            Index_p 
        );

        CREATE TABLE NewParent as select * from Parent where false;
        insert into Parent 
            SELECT  
                rec.index_m as index_m,
                rec_.index_m as Index_p 
            FROM rec inner join rec as rec_ on
                rec_.index_m <> rec.index_m and
                rec_.l <= rec.l and
                (rec_.l + rec_.w) >= (rec.l + rec.w) and
                rec_.t <= rec.t and
                (rec_.t + rec_.h) >= (rec.t + rec.h);                       

        --Вычисляю реальных родителей. Выбирается родитель с максимальным уровнем вложенности

        insert into rec_ur 
            SELECT distinct
                rec.index_m,
                case when Parent.index_m is null then -1 else Parent.Index_p end as index_p ,
                case when  ur.ur_vl is null then -1 else ur.ur_vl end as ur_vl 
            from rec 
                left join Parent on rec.index_m = Parent.index_m
                left join (
                    SELECT 
                        Parent.index_m ,
                        count(Parent.Index_p)  as ur_vl
                    FROM Parent
                    Group by  Parent.index_m) as ur 
                on ur.index_m = Parent.Index_p;

        insert into NewParent 
            select 
                t1.index_m, 
                t1.index_p  
            from rec_ur t1 left join rec_ur t2 on
                t1.index_m = t2.index_m and  t1.ur_vl < t2.ur_vl 
                where t2.index_p is null or t2.index_p is NULL;

        DELETE FROM Parent;

        DELETE FROM rec_ur;

        INSERT INTO Parent select * from NewParent;

        DROP TABLE NewParent; 

        CREATE TABLE NewRec as select * from rec where false;

        --обновляю исходные данные, где будут реальные родители
        INSERT INTO NewRec
            select distinct 
                rec.index_block as index_block,
                rec.index_m,
                Parent.index_p,
                rec.l,
                rec.t,
                rec.w,
                rec.h,
                rec.text 
            from rec inner join Parent on 
                Parent.index_m = rec.index_m;

        DROP TABLE Parent;

        DELETE FROM rec;

        INSERT INTO rec select * from NewRec;

        DROP TABLE NewRec;""")

def CreateBlocks(cur,for_all_list = 0):
    if for_all_list != 0:
        usl =' where rec.index_m != 0 '
    else:
        usl = """inner join rec as rec_p on 
                rec_.index_p = rec_p.index_m 
                where rec_p.index_p = -1 """

    cur.executescript("""
            
        CREATE TABLE par( 
             Index1,
             Index2 
        );
        
        CREATE TABLE udal( 
              Index1,
              Index2 
        );

        
        insert into par 
            SELECT  distinct
                rec.index_m as Index1,
                rec_.index_m as Index2 
            FROM delta,rec                           
            inner join rec as rec_ on
                rec_.index_m > rec.index_m and rec_.index_p = rec.index_p  and (
                (
                ( abs(rec.l + rec.w - rec_.l) <= delta.value
                or abs(rec_.l + rec_.w - rec.l) <= delta.value)
                and
                ((rec.t <= rec_.t and rec_.t <= rec.t + rec.h and rec.t + rec.h <= rec_.t + rec_.h) or 
                (rec_.t <= rec.t and rec.t <= rec_.t + rec_.h and rec_.t + rec_.h <= rec.t + rec.h) or
                (rec_.t <= rec.t and rec.t + rec.h <= rec_.t + rec_.h) or 
                (rec.t <= rec_.t and rec_.t + rec_.h <= rec.t + rec.h) )
                )
                or
                (
                (abs(rec.t + rec.h - rec_.t)  <= delta.value
                or abs(rec_.t + rec_.h - rec.t) <= delta.value)                
                and
                ((rec.l <= rec_.l and rec_.l <= rec.l + rec.w and rec.l + rec.w <= rec_.l + rec_.w) or 
                (rec_.l <= rec.l and rec.l <= rec_.l + rec_.w and rec_.l + rec_.w <= rec.l + rec.w) or
                (rec_.l <= rec.l and rec.l + rec.w <= rec_.l + rec_.w) or 
                (rec.l <= rec_.l and rec_.l + rec_.w <= rec.l + rec.w) )
                )
                )
             """ + usl)


    while bool(1):
        # получаю узлы которые соседствую друг с другом
        cur.executescript("""
            insert into par 
                SELECT  distinct 
                * 
                from(
                    SELECT
                        par.Index1,
                        par_.Index2 
                    from par 
                    inner join par as par_ on
                        par.index2 = par_.index1 
                        and  par.index1 < par_.index1
                    union                      
                    SELECT
                        par.Index1,
                        par_.Index1 
                    from par inner join par as par_ on
                        par.index2 = par_.index2 
                        and  par.index1 < par_.index1);
            -- список узлов для удаления        
            insert into udal 
                select distinct
                    par.Index1,
                    par.Index2 
                    from par left join par as par1 
                        on par1.Index2 = par.Index1
                        and  par.index1 > par1.index1   
                    left join par as par2 on 
                        par2.Index2 = par.Index2 
                        and  par.index1 > par2.index1 
                    where  par2.index1 = par1.index1""")

        cur.execute("""select * from udal""");
        if len(cur.fetchall()) == 0:
            break
        cur.executescript("""
            CREATE TABLE Newpar as select * from par where false;

            INSERT INTO Newpar
                select  
                   par.Index1,
                   par.Index2
                from par left join udal on   
                   par.Index1 = udal.Index1 and
                   par.Index2 = udal.Index2 
                where udal.Index1 IS null;

            DELETE FROM par;

            INSERT INTO par SELECT * from Newpar;

            DELETE FROM udal;

            DROP TABLE Newpar;""")

    cur.executescript("""
        DROP TABLE udal;
        DELETE FROM rec_ur;

        --пары вида (1,1)
        insert into par 
            SELECT  distinct 
                par.Index1,
                par.Index1 
            from par;
            
        CREATE TABLE NewRec as select * from rec where false;

        --теперь можно окончательно собрать основную таблицу
        insert into NewRec
            select distinct 
                case when par.Index1 is NULL then rec.index_block else par.Index1 end  as index_block,
                rec.index_m,
                rec.index_p,
                rec.l,
                rec.t,
                rec.w,
                rec.h ,
                rec.text
            from rec left join par on 
                rec.index_m = par.Index2;

        DELETE FROM rec;

        INSERT INTO rec SELECT * FROM NewRec;

        DROP TABLE NewRec;""")

    if for_all_list == 0:
        # Теперь надо исключить самые маленькие поля, которые по статистике не влазят. Это мусор, который случайно прилип
        cur.execute("""update rec set index_block = 0
                where
                (index_block>0
                and index_block in
                (select rec.index_block from rec
                where index_block>0
                GROUP BY index_block
                    HAVING count(index_m) < 4
                )) or (index_block>0 and index_p<0)""")

        del_id_block = []

        cur.execute("""select index_block, index_m, h from  rec 
                   where index_block>0 
                    order by index_block, h""")
        all_block = cur.fetchall()
        # беру 2 самых маленьких элемента категории блок. Одна категория - значит что разница самого маленького и самого крупного должна быть не более 40% от характеристики самого маленького элемента.
        # если видно явное отличие одной категории от другой и мощность первого сильно меньше мощности второго, то удаляю всю первую категорию из связанных ячеек

        trash_t = DetectTrash(all_block)
        cur.execute("""select index_block, index_m, w from  rec 
                           where index_block>0 
                            order by index_block, w""")
        all_block = cur.fetchall()
        trash_w = DetectTrash(all_block)
        #Если узлы в категориях пересекаются, то удалить
        for trash in trash_t:
             if trash_w.get(trash) != None:
                 for uzel in trash_w[trash]:
                     if uzel in trash_t[trash]:
                         del_id_block.append([uzel])
        for trash in trash_w:
             if trash_t.get(trash) != None:
                 for uzel in trash_t[trash]:
                     if uzel in trash_w[trash]:
                         del_id_block.append([uzel])
        cur.execute("""create table trash (index_block INT) """)
        cur.executemany("INSERT INTO trash VALUES(?);", del_id_block)
        cur.execute("""update rec set index_block = 0 where index_m in (select * from trash)""")
        cur.execute("""DROP table trash """)
    cur.execute("""DROP TABLE par """)

def DetectTrash(all_block):
    tec_bl = None
    spis = []
    all_spis = []
    last_h = 0
    ras_bl = []
    i = 0
    result = {}
    while i < len(all_block):
        block = all_block[i]

        if len(all_spis) == 2:
            ras_bl.append(all_spis[0][1])
            if len(all_spis[0][0]) < len(all_spis[1][0]) * 0.6:
                result[str(tec_bl)] = all_spis[0][0]
            all_spis = []
            spis = []

        if tec_bl == None:
            tec_bl = block[0]
        if tec_bl != block[0]:
            if len(spis) > 0:
                all_spis.append([spis, tec_bl, last_h])
                continue
            all_spis = []
            last_h = 0;
            tec_bl = block[0];
        if tec_bl in ras_bl:
            i = i + 1
            continue
        if last_h == 0:
            last_h = block[2]
        if last_h <= block[2] <= last_h * 1.4:
            # Принадлежит категории
            spis.append(block[1])
        else:
            if len(spis) > 0:
                all_spis.append([spis, tec_bl, last_h])
            spis = []
            last_h = block[2];
            tec_bl = block[0];
            spis.append(block[1])
        i = i + 1
    return result

def PihtoKategor(cur,s1,s2,ind_block):
    #функция получает нужные координаты ячеек и распредляет по категориям. Разница между первым и вторым элементами не более 2*delta
    cat_lt = []
    if ind_block == 0:
        #тут нужно получить границы блоков
        cur.execute("""
            select %s as lt from gr_img            
            union
            select %s as lt from gr_img            
            union
            select distinct min(%s) as l_t from  rec 
            where index_block <> :index_block
            group by index_block
            union
            select distinct max(%s) as l_t from  rec 
            where index_block <> :index_block
            group by index_block
            order by l_t""" % (s1, s2,s1, s2), {"index_block": ind_block})
    else:
        # тут нужно получить границы ячеек отдельного блока
        cur.execute("""
            select distinct %s as l_t from  rec 
            where index_block = :index_block
            union
            select distinct %s as l_t from  rec 
            where index_block = :index_block
            order by l_t""" % (s1, s2), {"index_block": ind_block})
    all_lt = cur.fetchall()

    last_lt = -2*delta-1
    mas = []
    for lt in all_lt:
        if last_lt == -2*delta-1:
            last_lt = lt[0]

        if not (lt[0] - last_lt <= 2 * delta):
            if len(mas) == 1:
                cat_lt.append([ind_block, mas[0]])
            else:
                sred_lt = round((mas[len(mas)-1] + mas[0])/2)
                if sred_lt == mas[0]:
                    sred_lt = sred_lt+delta

                cat_lt.append([ind_block, sred_lt])
            last_lt = lt[0]
            mas = []
            mas.append(last_lt)
        else:
            mas.append(lt[0])
    if last_lt != 0:
        if len(mas) == 1:
            cat_lt.append([ind_block, mas[0]])
        else:
            sred_lt = round((mas[len(mas) - 1] + mas[0]) / 2)
            if sred_lt == mas[0]:
                sred_lt = sred_lt + delta
            cat_lt.append([ind_block, sred_lt])

    return cat_lt

def BreakOnLine(cur, vert = 1, for_all_list = 0):
    #Данная процедура получает все горизонтали/вертикали. берет 2 соседние, получаею все блоки, которые попадаются в данный промежуток
    #таким образом я могу понять от какого блока к какому я могу провести прямоугольники
    #Процедура запускается 2 раза, один раз для вертикалей, второй раз для горизонталей.
    #После чего я накладываю полученные прямоугольники, группирую при необходимости и PROFIT
    # если for_all_list, то каждый отдельный блок нужно смотреть как отдельную ячейку, а родитель - это весь лист. Таким образом набираю промежутки между таблицами
    dop_bl = []
    if for_all_list != 0:
        all_results = [[0]]
    else:
        cur.execute("""select distinct index_block from  rec 
           where index_block>0 and index_p>0  
           and index_block in
           (select rec.index_block from rec
           GROUP BY index_block 
               HAVING count(index_m) > 3
           )
            order by index_block""")
        all_results = cur.fetchall()
    if vert == 1:
        s1 = 'l'
        s2 = 'l+w'
    else:
        s1 = 't'
        s2 = 't+h'

    for ind_block in all_results:
        # теперь набираю линии в категории, разница между первым и последним элеметом не более 10
        cat_lt = PihtoKategor(cur,s1,s2,ind_block[0])
        if for_all_list != 0:
            cur.execute(""" select
                    0,
                    gr_img.l as l,
                    gr_img.t as t,
                    gr_img.w as lw,
                    gr_img.h as th
                    from gr_img as gr_img""")
        else:
            cur.execute(""" select
                    rec.index_block,
                    min(rec.l) as l,
                    min(rec.t) as t,
                    max(rec.l+rec.w) as lw,
                    max(rec.t+rec.h) as th
                    from rec as rec
                    where index_block = :index_block
                    group
                    by
                    index_block""", {"index_block": ind_block[0]})
        gr = cur.fetchall()
        gr = gr[0]
        if vert == 0:
            if for_all_list != 0:
                text_z = """select distinct index_block as index_m, t, th, l, lw  from  delta,
                (select
                     rec.index_block,
                     min(rec.l) as l,
                     min(rec.t) as t,
                     max(rec.l+rec.w) as lw,
                     max(rec.t+rec.h) as th
                 from rec as rec
                 where index_block > 0
                 group
                 by
                 index_block) as rec_ 
                where
                abs(rec_.t - :lt1) <= 2*delta.value or
                abs(rec_.th - :lt2) <= 2*delta.value or
                ((rec_.th >  :lt2 + delta.value) and (rec_.t < :lt1 - delta.value))                    
                order by l"""
            else:
                text_z = """select distinct index_m, t, t + h, l, l+w  from  delta, rec 
                               where index_block = :index_block and (
                               abs(t - :lt1) <= 2*delta.value or
                               abs(t+h - :lt2) <= 2*delta.value or
                               ((t+h >  :lt2 + delta.value) and (t < :lt1 - delta.value)) )                   
                               order by l"""
            int_lt_gr = 1
            int_lt_gr_last = 3
            int_lt_p = 3
            int_lt_v = 4
        else:
            if for_all_list != 0:
                text_z = """select distinct index_block as index_m, t, th, l, lw  from  delta, 
                    (select
                     rec.index_block,
                     min(rec.l) as l,
                     min(rec.t) as t,
                     max(rec.l+rec.w) as lw,
                     max(rec.t+rec.h) as th
                     from rec as rec
                     where index_block > 0
                     group
                     by
                     index_block) as rec_ 
                   where  
                   abs(l - :lt1) <= 2*delta.value or
                   abs(lw - :lt2) <= 2*delta.value or
                   ((lw >  :lt2 + delta.value) and (l < :lt1 - delta.value))                    
                   order by t"""
            else:
                text_z = """select distinct index_m, t, t +h, l, l+w  from  delta, rec 
                               where index_block = :index_block and (
                               abs(l - :lt1) <= 2*delta.value or
                               abs(l+w - :lt2) <= 2*delta.value or
                               ((l+w >  :lt2 + delta.value) and (l < :lt1 - delta.value)) )                   
                               order by t"""
            int_lt_gr = 2
            int_lt_gr_last = 4
            int_lt_p = 1
            int_lt_v= 2
        for i in range(0, len(cat_lt) - 1):
            lt1 = cat_lt[i][1]
            lt2 = cat_lt[i + 1][1]
            # теперь мне надо узнать какие блоки лежат на данной прямой, можно ли туда сунуть новый прямоугольник и с какой стороны какие блоки лежат
            cur.execute(text_z, {"index_block": cat_lt[i][0], "lt1": lt1, "lt2": lt2})
            qwe = cur.fetchall()
            # теперь мне нужно узнать, какие блоки я могу создать
            pred_in = -1
            pred_lt = gr[int_lt_gr]
            for _b in qwe:
                if _b[int_lt_p] > pred_lt + 2*delta:
                    # от границы до тек блока
                    if vert == 0:
                        dop_bl.append([cat_lt[i][0], pred_in, _b[0],  pred_lt,lt1,_b[int_lt_p], lt2])
                    else:
                        dop_bl.append([cat_lt[i][0], pred_in, _b[0], lt1, pred_lt, lt2, _b[int_lt_p]])
                pred_lt = _b[int_lt_v]
                pred_in = _b[0]
            if gr[int_lt_gr_last] > pred_lt + 2*delta:
                # от тек блока до границы
                if vert == 0:
                    dop_bl.append([cat_lt[i][0], pred_in, -1, pred_lt, lt1, gr[int_lt_gr_last], lt2])
                else:
                    dop_bl.append([cat_lt[i][0], pred_in, -1, lt1, pred_lt, lt2, gr[int_lt_gr_last]])

    cur.executescript("""
        CREATE TABLE new_block_%s( 
            index_block INT ,
            index_m_l INT,
            index_m_p INT,
            l int,
            t int,
            lw int,
            th int)""" %(s1))
    cur.executemany("INSERT INTO new_block_%s VALUES(?,?, ?, ?, ?, ?, ?);"%(s1), dop_bl)

    cur.executescript("""
        CREATE TABLE new_block_%s_ as select * from new_block_%s where false;
        insert into new_block_%s_ 
            select 
                index_block ,
                index_m_l,
                index_m_p,
                min(l),
                min(t),
                max(lw),
                max(th)
            from new_block_%s   
            where not (index_m_l = -1 and index_m_p = -1)
            group by 
                index_block,
                index_m_l,
                index_m_p
            union
            select 
                index_block ,
                index_m_l,
                index_m_p,
                l,
                t,
                lw,
                th
            from new_block_%s   
            where (index_m_l = -1 and index_m_p = -1)
            ;
            delete from  new_block_%s;
            insert into new_block_%s select * from new_block_%s_;
            drop table new_block_%s_;
        """%(s1,s1,s1,s1,s1,s1,s1,s1,s1))

def FillToFullRec(cur,len_coordinates):
    BreakOnLine(cur, 0)
    BreakOnLine(cur)

    cur.executescript("""
        CREATE TABLE almost_end_block( 
                index_block INT ,
                index_p INT ,
                index_m_l INT,
                index_m_lw INT,
                index_m_t INT,
                index_m_th INT,
                l int,
                t int,
                lw int,
                th int
                );

        insert into almost_end_block        
            select new_block_l.index_block  ,
                rec.index_p ,
                new_block_l.index_m_l ,
                new_block_l.index_m_p ,
                new_block_t.index_m_l ,                 
                new_block_t.index_m_p ,                 
                new_block_t.l as l,
                new_block_t.t as t,
                new_block_l.lw as lw,
                new_block_t.th as th
            from  delta, new_block_l left join rec on
                new_block_l.index_block = rec.index_block and 
                --по горизонтали блоки соседние
                abs(rec.l - new_block_l.lw)<= 2*delta.value and
                --по вертикали соседние блоки начинаются или заканчиваются в текущем
                --или ткущий полностью в блоке
                (
                ((rec.t >=  new_block_l.t + delta.value) and (rec.t <=  new_block_l.th - delta.value)) or
                ((rec.t+rec.h >=  new_block_l.t + delta.value) and (rec.t+rec.h <=  new_block_l.th - delta.value)) or
                ((rec.t <=  new_block_l.t + delta.value) and (rec.t +rec.h >=  new_block_l.th - delta.value))                 
                )
                  
            inner join new_block_t on 
                rec.index_m = new_block_t.index_m_p   
            union
            select new_block_l.index_block  ,
                rec.index_p ,
                new_block_l.index_m_l ,
                new_block_l.index_m_p ,
                new_block_t.index_m_l ,                 
                new_block_t.index_m_p ,                 
                new_block_t.l as l,
                new_block_t.t as t,
                new_block_t.lw as lw,
                new_block_t.th as th
            from  delta, new_block_l left join rec on
                new_block_l.index_block = rec.index_block and 
                    --((rec.l+rec.w >=  new_block_l.l - delta.value) and (rec.l+rec.w <=  new_block_l.l + delta.value)) and
                    abs(rec.l+rec.w -  new_block_l.l)<= 2*delta.value  and
                    (
                    ((rec.t >= new_block_l.t + delta.value) and (rec.t <=  new_block_l.th - delta.value)) or
                    ((rec.t+rec.h >= new_block_l.t + delta.value) and (rec.t+rec.h <=  new_block_l.th - delta.value)) or
                    ((rec.t <=  new_block_l.t + delta.value) and (rec.t +rec.h >=  new_block_l.th - delta.value))                 
                    )  
            inner join new_block_t on 
                rec.index_m = new_block_t.index_m_l;
        DROP table  new_block_l;
        DROP table  new_block_t;     
        """)

    # группированные таблицы. Осталось их убрать из основной и получаю все нужные блоки до достройки до полноценного прямоугольника
    cur.executescript("""
        CREATE TABLE end_block_gr( 
                index_block INT ,
                index_p INT ,
                index_m_l INT,
                index_m_lw INT,
                l int,
                t int,
                lw int,
                th int
                );
        Insert into end_block_gr        
            SELECT 
                index_block,
                index_p ,
                index_m_l,
                index_m_lw,
                l,
                t,
                lw,
                th
            from delta,
                (select 
                    index_block ,
                    index_p ,
                    index_m_l ,
                    index_m_lw ,            
                    min(l) as l,
                    min(t) as t,
                    max(lw) as lw,
                    max(th) as th 
                from almost_end_block 
                group by
                    index_p ,
                    index_block ,
                    index_m_l ,
                    index_m_lw) as vl
                WHERE (lw-l) /(th -t) <= 5 and ((lw-l) /(th -t)) >=(0.2)
                and not index_p is null
                 
        """)

    cur.execute("""
        SELECT 
            index_block,
            index_p,
            l,
            t,
            lw,
            th
        from delta,end_block_gr
        where lw-l > 2*delta.value and th-t > 2*delta.value
        and not index_p is null
        union
        SELECT 
            almost_end_block.index_block ,
            almost_end_block.index_p ,
            almost_end_block.l ,
            almost_end_block.t ,
            almost_end_block.lw ,
            almost_end_block.th 
        from delta, almost_end_block left join end_block_gr on
            almost_end_block.index_block = end_block_gr.index_block
            and almost_end_block.index_m_l = end_block_gr.index_m_l
            and almost_end_block.index_m_lw = end_block_gr.index_m_lw
        where end_block_gr.index_block is null
        and  almost_end_block.lw-almost_end_block.l > 2*delta.value and almost_end_block.th-almost_end_block.t > 2*delta.value 
        and not almost_end_block.index_p is null""")

    all_results = cur.fetchall()
    NewRec = []
    for new in all_results:
        index_block, index_p, l, t, w, h = new
        NewRec.append((index_block, len_coordinates, index_p, l, t, w - l, h - t,''))
        len_coordinates = len_coordinates + 1
    cur.executemany("INSERT INTO rec VALUES(?,?, ?, ?, ?, ?, ?, ?);", NewRec)

    cur.execute("drop table almost_end_block")
    cur.execute("drop table end_block_gr")

def rast_block(cur):
    cur.executescript("""
        CREATE TABLE gran( 
             index_block int,
             index_p int,
             l int,
             t int,
             lw int,
             th int,
             lp int,
             tp int,
             lwp int,
             thp int 
          );
        insert into gran
            select
                rec.index_block,
                rec.index_p,
                min(rec.l) as l,
                min(rec.t) as t,
                max(rec.l+rec.w) as lw,
                max(rec.t+rec.h) as th,
                max(rec_p.l) as lp,
                max(rec_p.t) as tp,
                max(rec_p.l+rec_p.w) as lwp,
                max(rec_p.t+rec_p.h) as thp
            from rec as rec inner join rec as rec_p on
                rec_p.index_m = rec.index_p
            where rec.index_block>0 and  rec.index_p >0
            group by
                rec.index_block,
                rec.index_p
            """)

    cur.execute(""" 
        select
            rec.index_m, 
            gran.lp,                         
            rec.l - gran.lp + rec.w                         
        from delta, gran 
            inner join  rec as rec
        where rec.index_block = gran.index_block and rec.l between gran.l - 2*delta.value and gran.l + 2*delta.value
            and ABS(gran.l - gran.lp) < ABS(gran.lw - gran.l)/3""")
    gr = cur.fetchall()
    for gr_ in gr:
        cur.execute("""update rec set l = %s, w = %s where index_m = %s""" % (gr_[1], gr_[2], gr_[0]))

    cur.execute(""" 
        select
            rec.index_m, 
            gran.tp,                         
            rec.t - gran.tp + rec.h                         
        from delta, gran 
            inner join  rec as rec
        where rec.index_block = gran.index_block and rec.t between gran.t - 2*delta.value and gran.t + 2*delta.value
            and ABS(gran.t - gran.tp) < ABS(gran.th - gran.t)/3""")
    gr = cur.fetchall()
    for gr_ in gr:
        cur.execute("""update rec set t = %s, h = %s where index_m = %s""" % (gr_[1], gr_[2], gr_[0]))

    cur.execute(""" 
        select
            rec.index_m, 
            gran.lwp - rec.l                         
        from delta, gran 
            inner join  rec as rec
        where rec.index_block = gran.index_block and rec.l + rec.w between gran.lw - 2*delta.value and gran.lw + 2*delta.value
            and ABS(gran.lw - gran.lwp) < ABS(gran.lw - gran.l)/3""")
    gr = cur.fetchall()
    for gr_ in gr:
        cur.execute("""update rec set w = %s where index_m = %s""" % (gr_[1], gr_[0]))

    cur.execute(""" 
        select
            rec.index_m, 
            gran.thp - rec.t                        
        from delta, gran 
            inner join  rec as rec
        where rec.index_block = gran.index_block and rec.t + rec.h between gran.th - 2*delta.value and gran.th + 2*delta.value
            and ABS(gran.th - gran.thp) < ( ABS(gran.th - gran.t))/3""")
    gr = cur.fetchall()
    for gr_ in gr:
        cur.execute("""update rec set h = %s where index_m = %s""" % (gr_[1], gr_[0]))

def InsertTable(cur, len_coordinates, img):
    # Для начала вычисляется уровень вложенности
    FillRealParent(cur)

    # задается предел отклонения
    cur.executescript("""
        CREATE TABLE delta( 
             value 
          );

        insert into delta select %s; 
        """ % (str(delta)))

    #вычисляю блоки ячеек, которые связаны между собой
    CreateBlocks(cur)

    # cur.execute("""select * from rec where index_block>0 and index_p>0 """)
    # all_results = cur.fetchall()
    # Output1 = img.copy()
    # for qwe in all_results:
    #     block, ind, p, l, t, w, h, txt = qwe
    #     cv2.rectangle(Output1, (l, t), (l + w, t + h), (125, 125, 255), 2)
    #     cv2.putText(Output1, str(ind), (l, round((t + t + h) / 2) + 10), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
    #
    # width = int(Output1.shape[1] * 30 / 100)
    # height = int(Output1.shape[0] * 30 / 100)
    # dim = (width, height)
    # resized = cv2.resize(Output1, dim, interpolation=cv2.INTER_AREA)
    # cv2.imshow("Output1", resized)
    # cv2.waitKey(0)


    #нужно самые крайние блоки дотянуть до границы родителя, если эти блоки находятся недалеко от границы родителя
    rast_block(cur)
    #по возможности достраиваю таблицу до полного прямоугльника
    FillToFullRec(cur,len_coordinates)
    #удаляю те блоки, которые не получилось дотянуть до полного прямоугольника
    #у таких сумма всех длин граничных блока не +- равна границе
    # cur.execute("""select * from rec
    #                    where  index_p>0 and index_block>0
    #                     order by index_m""")
    # all_results = cur.fetchall()
    # cur.execute("""
    #         select *
    #             from (
    #                 select
    #                     vl.index_block,
    #                     max(CAST(vl.th-vl.t as float)) as r_t,
    #                     max(CAST(vl.lw - vl.l as float)) as r_l,
    #                     sum(CAST(case when abs(rec.t - vl.t)<= delta.value then  rec.w else 0 end as float)) as sum_lt,
    #                     sum(CAST(case when abs(rec.t + rec.h - vl.th)<= delta.value then  rec.w else 0 end as float)) as sum_lb,
    #                     sum(CAST(case when abs(rec.l - vl.l)<= delta.value then  rec.h else 0 end as float)) as sum_tl,
    #                     sum(CAST(case when abs(rec.l+rec.w - vl.lw)<= delta.value then  rec.h else 0 end as float)) as sum_tr
    #                 from
    #                     delta,
    #                     (select index_block, min(l) as l, min(t) as t, max(l+w) as lw, max(t+h) as th from  rec
    #                     where index_block>0
    #                     group by index_block) as vl left join rec on
    #                     vl.index_block =  rec.index_block
    #                     group by vl.index_block) as vl
    #                 Where
    #                     vl.sum_lt/vl.r_l < 0.90
    #                     or vl.sum_lb/vl.r_l < 0.90
    #                     or vl.sum_tl/vl.r_t < 0.90
    #                     or vl.sum_tr/vl.r_t < 0.90
    #                     or vl.sum_lt/vl.r_l > 1.1
    #                     or vl.sum_lb/vl.r_l > 1.1
    #                     or vl.sum_tl/vl.r_t > 1.1
    #                     or vl.sum_tr/vl.r_t > 1.1
    #
    #             """)
    # all_results1 = cur.fetchall()
    # cur.execute("""select * from rec
    #                       where  index_ddd>0 and index_block>0
    #                        order by index_m""")
    cur.execute("""
        update rec set index_block = 0 where index_block in (
            select vl.index_block 
            from (
                select
                    vl.index_block,
                    max(CAST(vl.th-vl.t as float)) as r_t,
                    max(CAST(vl.lw - vl.l as float)) as r_l,
                    sum(CAST(case when abs(rec.t - vl.t)<= delta.value then  rec.w else 0 end as float)) as sum_lt,
                    sum(CAST(case when abs(rec.t + rec.h - vl.th)<= delta.value then  rec.w else 0 end as float)) as sum_lb,
                    sum(CAST(case when abs(rec.l - vl.l)<= delta.value then  rec.h else 0 end as float)) as sum_tl,
                    sum(CAST(case when abs(rec.l+rec.w - vl.lw)<= delta.value then  rec.h else 0 end as float)) as sum_tr
                from
                    delta,
                    (select index_block, min(l) as l, min(t) as t, max(l+w) as lw, max(t+h) as th from  rec 
                    where index_block>0
                    group by index_block) as vl left join rec on
                    vl.index_block =  rec.index_block
                    group by vl.index_block) as vl
                Where
                    vl.sum_lt/vl.r_l < 0.90
                    or vl.sum_lb/vl.r_l < 0.90
                    or vl.sum_tl/vl.r_t < 0.90
                    or vl.sum_tr/vl.r_t < 0.90
                    or vl.sum_lt/vl.r_l > 1.05
                    or vl.sum_lb/vl.r_l > 1.05
                    or vl.sum_tl/vl.r_t > 1.05
                    or vl.sum_tr/vl.r_t > 1.05
            )
            """)
    #так как появились новые, то заново нужно сделать
    FillRealParent(cur)
    #уберем индекс блока, если блок находится внутри прямоугольника, у котрого родитель имеет родителя
    cur.execute("""update rec set index_block = 0 where not index_m in 
        (Select 
            rec.Index_m
        from rec inner join rec as rec_ on
            rec.index_p = rec_.index_m
        where rec.index_block <> 0 and rec_.index_p = -1         
        )""")

    #тест на показ новый прямоугольников
    #all_results = cur.fetchall()

    # cur.execute("""select * from  rec
    #         where index_block>0 and index_p>0
    #          order by index_block, h""")
    # all_results = cur.fetchall()
    #
    # all_ = 0
    # output = img.copy()
    # for qwe in all_results:
    #     block, ind, p, l, t, w, h = qwe
    #
    #     all_ = block + 5
    #
    #     all_ = all_ + 1
    #     col_r = 50 * all_ % 255
    #     col_g = 30 * (all_ + 2) % 255
    #     col_b = 40 * (all_ + 4) % 255
    #     cv2.rectangle(output, (l, t), (l + w, t + h), (col_r, col_g, col_b), 2)
    #     cv2.putText(output, str(ind), (l, round((t + t + h) / 2) + 10), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
    #
    #     # break
    #
    # width = int(output.shape[1] * scale_percent / 100)
    # height = int(output.shape[0] * scale_percent / 100)
    # dim = (width, height)
    # resized = cv2.resize(output, dim, interpolation=cv2.INTER_AREA)
    #
    # cv2.imshow("Output1" + str(all_), resized)

def sum_col_raw_span(graph,tek,napr, dop = []):
    sum = 0
    if napr == 'b':
        kontrNapr = 't'
        par = 'colspan'
    elif napr == 't':
        kontrNapr = 'b'
        par = 'colspan'
    elif napr == 'r':
        kontrNapr = 'l'
        par = 'rowspan'
    elif napr == 'l':
        kontrNapr = 'r'
        par = 'rowspan'

    for uzel in tek['ed_'+napr]:
        if uzel in dop:
            continue
        if len(graph[uzel]['ed_'+kontrNapr]) == 1:
            sum = sum + graph[uzel][par]
        else:
            dop1 = dop.copy()
            dop1.append(tek['vertices'])
            sum = sum + graph[uzel][par] - sum_col_raw_span(graph,graph[uzel],kontrNapr,dop1)

    return sum

def correct_col_raw_span(graph):
    #сейчас colspan и rowspan это просто максимальное количество узлов с одной стороны. Это явно неправильно, нужно пересчитать
    was = 1
    prov = []
    prov.append(['b', 'colspan'])
    prov.append(['t', 'colspan'])
    prov.append(['l', 'rowspan'])
    prov.append(['r', 'rowspan'])

    while was == 1:
        was = 0
        for bl in graph:
            #вычисляю colspan блоков снизу, если их общее количество не равно текущему colspan то меняю на разницу текущий и все верхне/нижне/сбоку лежащие
            #проблема в вычислении, потому что блоки снизу могут выходить за рамки текущего блока, тогда если блок имеет только одного соседнего - текущего (в заданном направлении)
            #то плюсую весь параметр, если нет то параметр - часть которая выходит за ткущий блок, вычисляется рекурсивно.
            for pr in prov:
                ras = sum_col_raw_span(graph,bl,pr[0])
                if ras > 1000:
                    # Какое то зацикливание
                    was = 0
                    return []
                elif ras>0 and ras < bl[pr[1]]:
                     uzel = graph[bl['ed_'+pr[0]][0]]
                     uzel[pr[1]] = uzel[pr[1]] + bl[pr[1]] - ras
                     was = 1
                elif  ras>0 and ras > bl[pr[1]]:
                    was = 1
                    bl[pr[1]] = ras

    return graph

def RecInsertToShap(tek,shap,spis_uzl,len_shap,graph,nak_len,nak_text =''):

    if tek in spis_uzl:
        return None
    edges_b = graph[tek]['ed_b']
    tek_text = nak_text+' '+graph[tek]['text']
    tek_len_shap = nak_len + graph[tek]['rowspan']
    if tek_len_shap == len_shap:
        shap.append({'vertices':tek})
        spis_uzl.append(tek)
    elif tek_len_shap > len_shap:
        return tek_len_shap
    else:
        for uzel in edges_b:
            tek_nak_len = tek_len_shap + nak_len
            result = RecInsertToShap(uzel,shap,spis_uzl,len_shap,graph,tek_nak_len,tek_text);
            if result != None:
                return result

def rec_opr_shap(tek,shap,spis_uzl,len_shap,graph,nak_len):

    result =  RecInsertToShap(tek,shap,spis_uzl,len_shap,graph,nak_len);
    if result!=None :
        return result

    edges_r = graph[tek]['ed_r']
    for uzel_r in edges_r:
        result = rec_opr_shap(uzel_r,shap,spis_uzl,len_shap,graph,nak_len)
        if result != None:
            return result

def len_all_tab(graph,tek):
    tek_bl= graph[tek]
    if len(tek_bl['ed_b']) == 0:
        return tek_bl['rowspan']
    else:
        sum = tek_bl['rowspan'] + len_all_tab(graph,tek_bl['ed_b'][0])
        return sum

def opr_shap(graph):
    #определяет шапку таблицы
    if len(graph)==0:
        return []

    len_shap = graph[0]['rowspan']
    len_all = len_all_tab(graph,0)
    i = 0
    find = 0
    shap = []
    spis_uzl = []
    while find == 0:
        # если высота шапки превышает высоту всей таблицы, то хрень
        if len_all < len_shap:
            return {}
        i = i + 1
        if i>20:
            return []
        result = rec_opr_shap(0, shap,spis_uzl, len_shap, graph, 0);
        if result == None:
            find = 1;
        elif type(result) == int:
            shap = []
            spis_uzl = []
            len_shap = result;
        else:
            return []

    for kolon in shap:
        graph[kolon['vertices']]['this_is_shap'] = 1

    return shap

def gettext(image, graph, vertices, lang, result):

    for uzel in vertices:
        # 6 лучше разбирает текст в несколько строк, стандарт 3 - длинные строки, 7 - цифры
        # теперь нужно сравнить и выбрать лучший текст
        t1 = graph[uzel]['t']
        t2 = graph[uzel]['th']
        l1 = graph[uzel]['l']
        l2 = graph[uzel]['lw']

        text1 = pytesseract.image_to_string(image[t1:t2, l1:l2], lang=lang, config='--psm 6')
        text2 = pytesseract.image_to_string(image[t1:t2, l1:l2], lang=lang, config='')
        text3 = pytesseract.image_to_string(image[t1 + round(delta / 2):t2 - round(delta / 2), l1 + round(delta / 2):l2 - round(delta / 2)], lang=lang, config='--psm 7')

        text1 = text1.replace("\n", " ")
        text2 = text2.replace("\n", " ")
        text3 = text3.replace("\n", " ")

        text1 = re.sub(' *[^ \(\)А-Яа-я\d\w\/\\\.\-,:; ]+ *', ' ', text1)
        text2 = re.sub(' *[^ \(\)А-Яа-я\d\w\/\\\.\-,:; ]+ *', ' ', text2)
        text3 = re.sub(' *[^ \(\)А-Яа-я\d\w\/\\\.\-,:; ]+ *', ' ', text3)

        while text1.find('  ')!=-1:
            text1 = text1.replace('  ',' ')
        while text2.find('  ') != -1:
            text2 = text2.replace('  ', ' ')
        while text3.find('  ') != -1:
            text3 = text3.replace('  ', ' ')

        text1 = text1.strip()
        text2 = text2.strip()
        text3 = text3.strip()
        #Если 2 из 3х равны и не пусто, то это нужный текст
        #если text3 - только цифры и знаки препинания, а в остальных что угодно, то это text3, но нужно учитывать размеры текстов
        #бывает, когда текст3 просто не распознаёт текст, а в реальности достаточно много символов
        #если text3 - буквы, а остальное, хотя бы одно, не пустое и содержит более 2х символов, то беру макс слово
        if text1 == text2 and text2==text3:
            text = text1
        elif (text1 == text2 or text1 == text3) and len(text1) > 2:
            text = text1
        elif text2 == text3 and len(text2) > 2:
            text = text2
        elif text3!='' and re.sub(' *[^ \-\d\.,]+ *', '', text3) == text3 and (len(text1) + len(text2))/2*0.4<len(text3) :
            text = text3
        elif text3!='' and len(text1) <= 2 and len(text2) <= 2:
            text = ''
        elif len(text1)>=len(text2) and len(text1)>2:
            text = text1
        elif len(text2) >= len(text1) and len(text2)> 2:
            text = text2
        else:
            text = ''
        graph[uzel]['text'] = text
        if text != '':
            if result != None:
                result[uzel] = 1

def get_table_structure(graph):
    # по получить граф тех элементов, которые распознались как шапка. Если шапки нет, то граф всей таблицы.
    # отличие от уже созданного графа, в том что здесь именно структура без текстовых данных и расположения соседних полей
    for i in range(-len(graph)+1,1):
        if graph[-i]['this_is_shap'] == 1:
            break
    if i == 0:
        #нужна вся таблица
        i = len(graph)
    else:
        i = -i +1

    spis = [y for y in range(0,i)]
    structure =  [(y,[]) for y in range(0,i)]

    for uzel in spis:
        for edges in graph[uzel]['ed']:
            if edges in spis:
                structure[uzel][1].append(edges)

    return  structure

def TableGraph(cur,image):
    #Сначала получаю все средние горизонтали таблиц
    cur.execute("""select distinct index_block from rec
               where index_block>0 and index_p>0
                order by t""")
    all_results = cur.fetchall()

    cur.execute("""select distinct * from rec
               where index_block>0 and index_p>0
                order by t""")
    all_results1 = cur.fetchall()

    cur.executescript("""
        CREATE TABLE t_rec(
           index_block INT ,
           t int);
        CREATE TABLE rec_whith_t_rec(
           index_block INT ,
           index_m INT ,
           l INT ,
           t INT ,
           lw INT ,
           th int,
           sred_t int)""")

    for block in all_results:
        cat_lt = PihtoKategor(cur, 't', 't', block[0])
        cur.executemany("INSERT INTO t_rec VALUES(?,?);", cat_lt)

    # этоу среднюю горизонталь проставлюю в блоки, по ней потом нужно будет сортироваться
    # нумирую в рамках группы каждый блок с 0, по количеству блоков которые находся левее и выше.
    # для каждого блока получаю список, которые лежат выше, ниже, слева, справа

    cur.executescript("""
        CREATE TABLE numeric_in_bl(
           index_block INT ,
           index_m INT ,
           num INT );

        INSERT INTO rec_whith_t_rec
            select
                rec.index_block, rec.index_m, rec.l, rec.t, rec.l + rec.w , rec.t + rec.h,t_rec.t
            from delta, rec inner join t_rec on
                rec.index_block =t_rec.index_block and
                abs(rec.t -t_rec.t) <=  delta.value;

        INSERT INTO numeric_in_bl
            select
                p1.index_block,
                p2.index_m ,
                count(p1.index_m)-1  as num    
            from
                (select distinct
                    index_block,
                    index_m,
                    l,
                    sred_t
                from rec_whith_t_rec   
                Order by sred_t,l) as p1
                 join (select distinct
                    index_block,
                    index_m,
                    l,
                    sred_t
                from rec_whith_t_rec   
                Order by sred_t,l) as p2
                on p2.index_block =p1.index_block and ((p1.l <= P2.l and p1.sred_t <= P2.sred_t) or p1.sred_t < P2.sred_t)
                group by
                    p1.index_block,
                    p2.index_m
                order by p1.index_block,num;

        CREATE TABLE almost_graph(
           index_block INT ,
           index_m INT ,
           l INT ,
           t INT ,
           lw INT ,
           th int,
           rec_l,
           rec_t,
           rec_lw,
           rec_th
           );

        insert INTO almost_graph    
            select 
                vl.index_block,
                numeric_in_bl.num as index_m,
                vl.l as l,
                vl.t as t,
                vl.lw as lw,
                vl.th as th,
                vl.rec_l, 
                vl.rec_t,
                vl.rec_lw,
                vl.rec_th 
            from(
                select distinct
                    vl.index_block,
                    vl.index_m,
                    vl.l as l,
                    vl.t as t,
                    vl.lw as lw,
                    vl.th as th,
                    numeric_in_bl.num as rec_l, 
                    NULL  as rec_t,
                    NULL  as rec_lw,
                    NULL  as rec_th         
                from    
                delta,rec_whith_t_rec as vl 
                left join rec  on 
                    vl.index_m<>rec.index_m
                    and vl.index_block = rec.index_block 
                    and abs(vl.l - rec.l -  rec.w)<=2*delta.value
                    and ( 
                    ((vl.t+delta.value <= rec.t) and (rec.t <= vl.th-delta.value)) or
                    ((vl.t+delta.value <= rec.t + rec.h) and (rec.t + rec.h <= vl.th-delta.value)) or
                    ((vl.t+delta.value >= rec.t) and (rec.t + rec.h >= vl.th-delta.value))
                    ) 
                inner join numeric_in_bl on numeric_in_bl.index_block = rec.index_block and numeric_in_bl.index_m = rec.index_m
                union      
                select distinct
                    vl.index_block,
                    vl.index_m,
                    vl.l as l,
                    vl.t as t,
                    vl.lw as lw,
                    vl.th as th,
                    NULL,                 
                    numeric_in_bl.num as rec_t ,
                    NULL,
                    NULL                  
                from 
                delta,rec_whith_t_rec as vl         
                left join rec  on 
                    vl.index_m<>rec.index_m
                    and vl.index_block = rec.index_block 
                    and abs(rec.t + rec.h - vl.t)<=2*delta.value
                    and ( 
                    ((vl.l+delta.value <= rec.l) and (rec.l <= vl.lw-delta.value)) or
                    ((vl.l+delta.value <= rec.l + rec.w) and (rec.l + rec.w <= vl.lw-delta.value)) or
                    ((vl.l+delta.value >= rec.l) and (rec.l + rec.w >= vl.lw-delta.value))
                    )
                inner join numeric_in_bl on numeric_in_bl.index_block = rec.index_block and numeric_in_bl.index_m = rec.index_m
                union      
                select distinct
                    vl.index_block,
                    vl.index_m,
                    vl.l as l,
                    vl.t as t,
                    vl.lw as lw,
                    vl.th as th,
                    NULL, 
                    NULL,
                    numeric_in_bl.num as rec_lw,
                    NULL        
                from 
                delta,rec_whith_t_rec as vl 
                left join rec  on 
                    vl.index_m<>rec.index_m
                    and vl.index_block = rec.index_block 
                    and abs(vl.lw - rec.l)<=2*delta.value
                    and ( 
                    ((vl.t+delta.value <= rec.t) and (rec.t <= vl.th-delta.value)) or
                    ((vl.t+delta.value <= rec.t + rec.h) and (rec.t + rec.h <= vl.th-delta.value)) or
                    ((vl.t+delta.value >= rec.t) and (rec.t + rec.h >= vl.th-delta.value))
                    )
                inner join numeric_in_bl on numeric_in_bl.index_block = rec.index_block and numeric_in_bl.index_m = rec.index_m
                union      
                select distinct
                    vl.index_block,
                    vl.index_m,
                    vl.l as l,
                    vl.t as t,
                    vl.lw as lw,
                    vl.th as th,
                    NULL, 
                    NULL,
                    NULL,
                    numeric_in_bl.num as rec_th                        
                from 
                delta,rec_whith_t_rec as vl 
                left join rec  on 
                    vl.index_m<>rec.index_m
                    and vl.index_block = rec.index_block 
                    and abs(rec.t - vl.th)<=2*delta.value
                    and ( 
                    ((vl.l+delta.value <= rec.l) and (rec.l <= vl.lw-delta.value)) or
                    ((vl.l+delta.value <= rec.l + rec.w) and (rec.l + rec.w <= vl.lw-delta.value)) or
                    ((vl.l+delta.value >= rec.l) and (rec.l + rec.w >= vl.lw-delta.value))
                    )
                inner join numeric_in_bl on numeric_in_bl.index_block = rec.index_block and numeric_in_bl.index_m = rec.index_m
            ) as vl 
            inner join numeric_in_bl on numeric_in_bl.index_block = vl.index_block and numeric_in_bl.index_m = vl.index_m           
                 """)
    # по итогу получаю список, где для каждого связанного блока прописан каждый блок с нумерацией с 0, с его положение и всеми связями как общими, так и по направлениям

    cur.execute("""
        select 
            graph.index_block,
            graph.index_m,
            rec_ltwh.l,
            rec_ltwh.t,
            rec_ltwh.lw,
            rec_ltwh.th,
            graph.gr,
            rec_ltwh.rec_l,
            rec_ltwh.rec_t,
            rec_ltwh.rec_lw,
            rec_ltwh.rec_th
        from
            (select 
                index_block,
                index_m,
                max(l) as l,
                max(t) as t,
                max(lw) as lw,
                max(th) as th,
                case when GROUP_CONCAT(rec_l) is null then '' else GROUP_CONCAT(rec_l) end as rec_l, 
                case when GROUP_CONCAT(rec_t) is null then '' else GROUP_CONCAT(rec_t) end as rec_t,
                case when GROUP_CONCAT(rec_lw) is null then '' else GROUP_CONCAT(rec_lw) end as rec_lw,
                case when GROUP_CONCAT(rec_th) is null then '' else GROUP_CONCAT(rec_th) end as rec_th                 
            from almost_graph     
            group by
                index_block,
                index_m
            order by
                index_block,
                index_m) as rec_ltwh 
        inner join (
            select 
                vl.index_block,
                vl.index_m,
                GROUP_CONCAT(vl.rec_) as gr
            from
                (select 
                    index_block,
                    index_m,
                    rec_l as rec_
                from almost_graph 
                union
                select 
                    index_block,
                    index_m,
                    rec_t  
                from almost_graph     
                union
                select 
                    index_block,
                    index_m,
                    rec_lw  
                from almost_graph     
                union
                select 
                    index_block,
                    index_m,
                    rec_th  
                from almost_graph
                order by  
                    index_block,
                    index_m,
                    rec_      
                ) as vl
            group by
                vl.index_block,
                vl.index_m ) as graph
        on graph.index_m = rec_ltwh.index_m and graph.index_block = rec_ltwh.index_block
        """)
    all_results = cur.fetchall()

    #теперь нужно привести это в вид для передачи в json и расспознать для каждой ячейки текст
    cur.executescript("""
        DROP TABLE numeric_in_bl;
        DROP TABLE almost_graph;
        DROP TABLE rec_whith_t_rec;
        DROP TABLE t_rec;
            """)
    list_gr = {}
    tek_bl = 0
    thsi_gr = []

    for bl in all_results:
        index_block, index_m, l, t, lw, th, gr, rec_l, rec_t, rec_lw, rec_th = bl

        if tek_bl == 0:
            tek_bl = index_block

        if tek_bl != index_block:
            graph = correct_col_raw_span(thsi_gr)
            if len(graph) > 0:
                shap = opr_shap(graph)
                #list_gr.append({'index_block':-1*tek_bl,'graph':graph,'shap':shap})
                list_gr[str(-1*tek_bl)] = {'graph':graph,'shap':shap, 'structure': get_table_structure(graph)}
            tek_bl = index_block
            thsi_gr = []
        dict = { }
        dict['vertices'] = index_m
        dict['ed'] = [int(item) for item in list(filter(None, gr.split(',')))]
        dict['ed_l'] = [int(item) for item in list(filter(None, rec_l.split(',')))]
        dict['ed_t'] = [int(item) for item in list(filter(None, rec_t.split(',')))]
        dict['ed_r'] = [int(item) for item in list(filter(None, rec_lw.split(',')))]
        dict['ed_b'] = [int(item) for item in list(filter(None, rec_th.split(',')))]
        dict['rowspan'] = max(len(dict['ed_l']),len(dict['ed_r']))
        dict['colspan'] = max(len(dict['ed_t']), len(dict['ed_b']))
        dict['this_is_shap'] = 0
        dict['t'] = t
        dict['th'] = th
        dict['l'] = l
        dict['lw'] = lw
        dict['text'] = ''
        thsi_gr.append(dict)
    if len(thsi_gr)>0:
        graph = correct_col_raw_span(thsi_gr)
        if len(graph)>0:
            shap = opr_shap(graph)
            #list_gr.append({'index_block':-1*tek_bl,'graph':graph,'shap':shap})
            list_gr[str(-1 * tek_bl)] = {'graph': graph, 'shap': shap, 'structure': get_table_structure(graph)}


    #всё, что не попало в графы - не таблицы, уберу индекс блока
    # нужно разбить на несколько потоков, что бы распознование было быстрее
    executor = conc.ThreadPoolExecutor(20)
    futures = []
    result = {}

    for graphs in list_gr:
        result[graphs] = [0 for i in range(0, len(list_gr[graphs]['graph']))]
        GetText_potok(image, executor, list_gr[graphs]['graph'], futures,'rus+eng',result[graphs])
    conc.wait(futures)

    for graphs in result:
        if max(result[graphs]) ==0:
            list_gr.pop(graphs)
    #проставлю данные шапок
    for graphs in list_gr:
        # MaxVer = -1
        # for shap in list_gr[graphs]['shap']:
        #     MaxVer = max(MaxVer, shap['vertices'])
        # VerInShap = [i for i in range(0, MaxVer + 1)]
        for shap in list_gr[graphs]['shap']:
            shap['sinonim'] = list_gr[graphs]['graph'][shap['vertices']]['text']
            shap['name'] = re.sub( "[^А-Яа-я\d\w]+", '',shap['sinonim'])
            shap['name'] = shap['name'].replace('%','Процент')
            shap['name'] = shap['name'].replace('№', 'Номер')
            shap['sinonim'] = re.sub(' *[^ \(\)А-Яа-я\d\w\/\\\.\-,:;]+ *', '', shap['sinonim'])
            while shap['sinonim'].find('  ') != -1:
                shap['sinonim'] = shap['sinonim'].replace('  ', ' ')
            # shap['kol'] = 1
            # VerInShap[shap['vertices']] = 0

        # for shap in VerInShap:
        #     if shap != 0:
        #         list_gr[graphs]['shap'].append({'vertices':shap,'sinonim':'','name':'','kol':0})

    return list_gr

def RecognizeTextField(cur,image):
    BreakOnLine(cur, 0, 1)

    cur.execute("select * from new_block_t order by t,l")
    spis = cur.fetchall()

    cur.executescript("""drop table new_block_t;
        CREATE TABLE NewRec as select * from rec where false;
        INSERT INTO NewRec
            select distinct 
                -1*rec.index_block as index_block,
                0,
                -1,
                min(rec.l),
                min(rec.t),
                max(rec.l+rec.w) - min(rec.l),
                max(rec.t+rec.h) - min(rec.t),
                rec.text                 
            from rec 
            where rec.index_block > 0  
            group by rec.index_block ;

        DELETE FROM rec;

        INSERT INTO rec select * from NewRec;

        DROP TABLE NewRec;
        """)

    # отсортирую прямоугольники, слишком узкие по краям уберу. удалю все прямоугльники из таблицы rec и заполню новыми
    cur.execute(""" select
                       gr_img.l as l,
                       gr_img.t as t,
                       gr_img.w as lw,
                       gr_img.h as th
                       from gr_img as gr_img""")
    gran = cur.fetchall()[0]

    new_rec = []

    for rec in spis:
        ib, ml, mp, l, t, lw, th = rec
        if l == gran[0] or lw == gran[2]:
            if lw - l >= (gran[2] - gran[0]) / 3:
                new_rec.append([0, len(new_rec) + 1, -1, l, t, lw - l, th - t, ''])
        else:
            new_rec.append([0, len(new_rec) + 1, -1, l, t, lw - l, th - t, ''])

    # распознаю текст
    graph = []
    for bl in new_rec:
        ib, im, ip, l, t, w, h, text = bl
        dict = {}
        dict['vertices'] = im
        dict['t'] = t
        dict['th'] = t + h
        dict['l'] = l
        dict['lw'] = l + w
        dict['text'] = ''
        graph.append(dict)

    # Output1 = image.copy()
    # for qwe in graph:
    #     ind,  t, h, l,w, txt = qwe
    #     cv2.rectangle(Output1, (qwe[l]+50, qwe[t]+50), ( qwe[w]-50, qwe[h]-50), (125, 125, 255), 2)
    #
    # width = int(Output1.shape[1] * 30 / 100)
    # height = int(Output1.shape[0] * 30 / 100)
    # dim = (width, height)
    # resized = cv2.resize(Output1, dim, interpolation=cv2.INTER_AREA)
    # cv2.imshow("Output1", resized)
    # cv2.waitKey(0)

    executor = conc.ThreadPoolExecutor(5)
    futures = []
    GetText_potok(image, executor, graph, futures, 'rus')
    conc.wait(futures)

    # заливаю в основную таблицу
    new_rec = []
    for dict in graph:
        if len(dict['text']) / (dict['lw'] - dict['l']) >= 0.01:
            new_rec.append(
                [0, dict['vertices'], -1, dict['l'], dict['t'], dict['lw'] - dict['l'], dict['th'] - dict['t'],
                 dict['text']])
    cur.executemany("INSERT INTO rec VALUES(?,?, ?, ?, ?, ?, ?, ?);", new_rec)

    # создаю блоки не таблиц
    CreateBlocks(cur, 1)

    # сливаю все блоки не таблиц в одну ячейку, если текста в ячейки мало, то удаляю её
    cur.executescript("""
        CREATE TABLE NewRec as select * from rec where false;
        INSERT INTO NewRec
            select
                0,
                index_,
                -1,
                l,
                t,
                w,
                h,
                text
                from
                (select 
                    case when index_block = 0 then index_m else index_block end as index_,
                    min(l) as l,
                    min(t) as t, 
                    max(l+w)- min(l) as w,
                    max(t+h)- min(t) as h,
                    GROUP_CONCAT(text , ' ') as text
                from rec where index_m != 0
                group by
                index_) as vl
                where cast(LENGTH(text)as float)/w >= 0.01;

        Delete from rec where index_m != 0;

        INSERT INTO rec select * from NewRec;

        DROP TABLE NewRec""")

    # По итогу в таблицу rec имею обшие поля с индексом блока = 0 и таблицы с отрицательным индексом блока.
    # сейчас нужно остортировать по t,l, пронумировать последовательно, сделать граф. Основная функция не подходит, так как
    # мне не нужно знать расположение слева/ справа и т.д, так как таблицы могут быть вложены в ячейки с общим текстом
    cur.executescript("""
        CREATE TABLE NewRec as select * from rec where false;
        INSERT INTO NewRec
            select
                rec.index_block,
                vl.num,
                rec.Index_p,
                rec.l,
                rec.t,
                rec.w,
                rec.h,
                rec.text
            from rec inner join (                 
            select
                p2.index_,
                count(p1.index_)-1  as num 
            from
                (select distinct
                    case when index_m = 0 then index_block else index_m end as index_,
                    l,
                    t
                from rec   
                Order by t,l) as p1
                 join (select distinct
                    case when index_m = 0 then index_block else index_m end as index_,
                    index_m,
                    l,
                    t
                from rec   
                Order by t,l) as p2
                on ((p1.l <= P2.l and p1.t <= P2.t) or p1.t < P2.t)
                group by
                   p2.index_
                order by num) as vl on
            case when rec.index_m = 0 then rec.index_block else rec.index_m end = vl.index_;

        Delete from rec ;

        INSERT INTO rec select * from NewRec;

        DROP TABLE NewRec""")


def recognzie(pathImg, tesseract_cmd, in_json = False):
    global delta
    pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
    img = cv2.imread(pathImg)
    #убираю штрихкода
    code = decode(img)
    for barcode in code:
        img[barcode.rect.top:barcode.rect.top+barcode.rect.height,barcode.rect.left:barcode.rect.left+barcode.rect.width] = np.array([255,255,255])

    # gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # ret, thresh = cv2.threshold(gray, 180, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    # kernel = np.ones((2, 2), np.uint8)
    # dilated_value = cv2.dilate(thresh, kernel, iterations=1)
    # # #thresh_value = 255 - cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]
    # dilated_value = cv2.GaussianBlur(dilated_value, (3,3), 0)

    clahe = cv2.createCLAHE(clipLimit=50, tileGridSize=(50, 50))
    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)  # convert from BGR to LAB color space
    l, a, b = cv2.split(lab)  # split on 3 different channels
    l2 = clahe.apply(l)  # apply CLAHE to the L-channel
    lab = cv2.merge((l2, a, b))  # merge channels
    img2 = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)  # convert from LAB to BGR

    gray = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    ret, thresh = cv2.threshold(gray, 75, 255, cv2.THRESH_BINARY_INV)

    kernel = np.ones((2, 2), np.uint8)
    obr_img = cv2.erode(thresh, kernel, iterations=1)

    obr_img = cv2.GaussianBlur(obr_img, (3, 3), 0)

    fhandle, fname = tempfile.mkstemp(suffix='.db', dir=tempfile.gettempdir())
    try:
        con = sl.connect(fname, uri=True, isolation_level=None, check_same_thread=False)
        #con = sl.connect('file::memory:?cache=shared', uri=True, isolation_level=None, check_same_thread=False)
        cur = con.cursor()

        contours, hierarchy = cv2.findContours(obr_img, cv2.RETR_TREE, cv2.CHAIN_APPROX_TC89_L1)
        coordinates = []
        ogr = round(max(img.shape[0], img.shape[1]) * 0.005)
        delta = round(ogr/2 +0.5)
        ind = 1;
        for i in range(0, len(contours)):
            l, t, w, h = cv2.boundingRect(contours[i])
            if (h > ogr and w > ogr):
                # в hierarchy номер родителя
                coordinates.append((0, ind, 0, l, t, w, h, ''))
                ind = ind + 1

        cur.executescript("""
                CREATE TABLE rec_ur(
                   index_m INT ,
                   index_p int,
                   ur_vl int  
                    );
            
                CREATE TABLE gr_img(
                   l int,
                   t int,
                   w int,
                   h int);
                   """)
        cur.executemany("INSERT INTO gr_img VALUES(?,?, ?, ?);", [[0,0,img.shape[1],img.shape[0]]])

        cur.execute("""
            CREATE TABLE rec(
               index_block INT ,
               index_m INT ,
               index_p int,
               l int,
               t int,
               w int,
               h int,
               text str)""")
        cur.executemany("INSERT INTO rec VALUES(?,?, ?, ?, ?, ?, ?,?);", coordinates)


        InsertTable(cur,len(coordinates) + 1,img)

        # cur.execute("""select * from rec
        #                    where  index_p = 1""")
        # all_results = cur.fetchall()
        # Output1 = img.copy()
        # for qwe in all_results:
        #     block, ind, p, l, t, w, h, txt = qwe
        #     cv2.rectangle(Output1, (l, t), (l + w, t + h), (125, 125, 255), 2)
        #     cv2.putText(Output1, str(ind), (l, round((t + t + h) / 2) + 10), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
        #
        # width = int(Output1.shape[1] * 30 / 100)
        # height = int(Output1.shape[0] * 30 / 100)
        # dim = (width, height)
        # resized = cv2.resize(Output1, dim, interpolation=cv2.INTER_AREA)
        # cv2.imshow("Output1", resized)
        # cv2.waitKey(0)

        # Получил связанные таблицы, теперь надо превратить их в граф и распознать текст.
        list_gr = TableGraph(cur, img)
        # Остаток страницы - это текст. Нужно вычленить связные кукски текста между таблицами. Один такой кусок - это одно вершина графа.
        # Таким образом, граф состоящий из кусков текста и шапок таблицы показывают что это за документ.
        # использую ту же самую технику, как и лдя таблиц, только теперь таблицы - это ячейки, родитель - весь лист, и не нужно достраивать
        RecognizeTextField(cur, img)

        cur.execute("select * from rec order by index_m")
        spis = cur.fetchall()
        # соединю все элементы друг за другом по порядку. Так как если делать стандартный граф, то слишком повышается вариабильность графа
        # и невозможно по структуре определить что это. к примеру пустые строки выкидываются, и могут быть варианты, когда текст с этой пустой строкой
        # пересекает таблицы, а так как она выкинута - то нет, что даёт изменчивый граф. Поэтому по порядку надежнее

        # это просто структура для быстрого поиска на равентсво
        structure = [[i, []] for i in range(0, len(spis))]
        # это уже готовые данные с необходимым количеством информации
        data = [[i, {}] for i in range(0, len(spis))]

        for i in range(0, len(spis)):
            ib, im, ip, l, t, w, h, text = spis[i]
            if list_gr.get(str(ib)) != None:
                data[i][1] = {'table': list_gr[str(ib)]['graph'], 'shapka': list_gr[str(ib)]['shap'],
                              'structure': list_gr[str(ib)]['structure']}
            else:
                data[i][1] = {'text': text}

            if list_gr.get(str(ib)) != None:
                structure[i][1] = list_gr[str(ib)]['structure']

            # if i + 1 < len(spis):
            #     structure[i][1].append(i + 1)
            #     structure[i + 1][1].append(i)

        # цикл если делать граф по пересечениям, оставлю пока
        # tek = 0
        # rasm = 1
        # while True:
        #     if rasm > len(spis)-1:
        #        break
        #     else:
        #         ib_tek, im_tek, ip_tek,l_tek,t_tek,w_tek,h_tek,text_tek = spis[tek]
        #         ib_rasm, im_rasm, ip_rasm, l_rasm, t_rasm, w_rasm, h_rasm, text_rasm = spis[rasm]
        #         if ((l_tek<= l_rasm <= l_tek + w_tek or l_tek<= l_rasm + w_rasm <= l_tek + w_tek or l_rasm<= l_tek<= l_tek + w_tek <= l_rasm + w_rasm) and
        #             (t_tek <= t_rasm <= t_tek + h_tek or t_tek <= t_rasm + h_rasm <= t_tek + h_tek or t_rasm <= t_tek <= t_tek + h_tek <= t_rasm + h_rasm)):
        #             # пересекаются
        #             structure[im_tek][1].append(im_rasm)
        #             structure[im_rasm][1].append(im_tek)
        #
        #             if list_gr.get(str(ib_rasm)) != None:
        #                 structure[im_rasm][2] = list_gr[str(ib_rasm)]['structure']
        #
        #             rasm = rasm + 1
        #         else:
        #             structure[im_rasm-1][1].append(im_rasm)
        #             structure[im_rasm][1].append(im_rasm-1)
        #             if list_gr.get(str(ib_rasm)) != None:
        #                 structure[im_rasm][2] = list_gr[str(ib_rasm)]['structure']
        #             tek = rasm
        #             rasm = rasm + 1

        cur.execute("""DROP TABLE rec""")
        con.close()
        result = {'data': data, 'structure': structure}
        os.close(fhandle)
        os.remove(fname)
        if in_json == True:
            return json.dumps(result, ensure_ascii=False)
        else:
            return result
    except:
        os.remove(fname)



if __name__ == "__main__":
    path = r'D:\qqqq_00012749.jpg'
    # path = r'D:\13_ 14.01.2019.jpg'
    # path = r'D:\SAMSUNG28032019_0002.jpg'
    # path = r'D:\DEEPCOM28022019.jpg'
    #
    # cv2.imshow("Output1", dilated_value)
    # cv2.waitKey(0)
    input()
    # print("Привет, {}!".format(namespace.file))
    res = recognzie(path,r'C:\Program Files\Tesseract-OCR\tesseract.exe')



