from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3
head_Html='http://www.taiwanlottery.com.tw/Lotto/Lotto649/history.aspx'
header_Item_name_list = ['期別','開獎日','兌獎截止','銷售金額','獎金總額']
header_Id_List = ['Lotto649Control_history_dlQuery_L649_DrawTerm_','Lotto649Control_history_dlQuery_L649_DDate_','Lotto649Control_history_dlQuery_L649_EDate_','Lotto649Control_history_dlQuery_L649_SellAmount_','Lotto649Control_history_dlQuery_Total_']
winning_Numbers_title_List = ['獎號1','獎號2','獎號3','獎號4','獎號5','獎號6','特別號']
winning_Numbers_Id = ['Lotto649Control_history_dlQuery_SNo1_','Lotto649Control_history_dlQuery_SNo2_','Lotto649Control_history_dlQuery_SNo3_','Lotto649Control_history_dlQuery_SNo4_','Lotto649Control_history_dlQuery_SNo5_','Lotto649Control_history_dlQuery_SNo6_','Lotto649Control_history_dlQuery_No7_']
winning_Numbers_Sort = ['Lotto649Control_history_dlQuery_No1_','Lotto649Control_history_dlQuery_No2_','Lotto649Control_history_dlQuery_No3_','Lotto649Control_history_dlQuery_No4_','Lotto649Control_history_dlQuery_No5_','Lotto649Control_history_dlQuery_No6_','Lotto649Control_history_dlQuery_SNo_']

def get_html(url):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return " ERROR "

def search_header_info(css_class):
    global header_Item_name_list,header_Id_List 
    if(css_class != None):
        for i in range(len(header_Item_name_list)):
            if header_Id_List[i] in css_class:
                return css_class
def search_winning_numbers(css_class):
    global winning_Numbers_Sort
    if(css_class != None):
        for i in range(len(winning_Numbers_Sort )):
            if winning_Numbers_Sort [i] in css_class:
                return css_class 
def insert_to_sql(lotto_header_dict_list,winning_Numbers_Dict,date_list):
    conn = sqlite3.connect('TW_Lotto_Winnings_Numbers_info.db')
    c = conn.cursor()
    createtablename = "tw_lotto_winnings_table"
    c.execute("create table  IF NOT EXISTS "+createtablename+" ('DrawTerm' integer PRIMARY KEY, 'DDate' text , 'EDate' text ,'SellAmount' integer,'Total' integer,'Number_No1' integer,'Number_No2' integer,'Number_No3' integer,'Number_No4' integer,'Number_No5' integer,'Number_No6' integer,'SpecialNumber_No7' integer)")
    for keys in date_list:
        lotto_header_dict_list[keys]
        winning_Numbers_Dict[keys]
        c.execute('INSERT OR IGNORE INTO '+createtablename+' values (?,?,?,?,?,?,?,?,?,?,?,?)',(lotto_header_dict_list[keys]+winning_Numbers_Dict[keys]))
    conn.commit()
    c.close()
def parse_tw_lotto_html(data_Info,data_Info_List,data_Info_Dict,data_Id_List,date_list = None):  
    for index  in range(len(data_Info)) :
        if (index == 0):
            data_Info_List.append(data_Info[index].text)  
        else:
            if(index % len(data_Id_List) != 0):
                data_Info_List.append(data_Info[index].text)
            else:
                if(date_list != None):
                    data_Info_Dict[date_list[int(index /len(data_Id_List))-1]] = list(data_Info_List)
                else:
                    data_Info_Dict[data_Info[index-len(data_Id_List)].text] = list(data_Info_List)
                data_Info_List= []
                data_Info_List.append(data_Info[index].text)
    return data_Info_List,data_Info_Dict
    
res= get_html(head_Html)
soup = BeautifulSoup(res,'lxml')
#print(soup.prettify())

header_Info = soup.find_all(id=search_header_info)
header_Info_List,header_Info_Dict = parse_tw_lotto_html(header_Info,[],{},header_Id_List)
  
winning_Numbers_Info = soup.find_all(id=search_winning_numbers)
date_list = list(header_Info_Dict.keys())
winning_Numbers_List,winning_Numbers_Dict = parse_tw_lotto_html(winning_Numbers_Info,[],{},winning_Numbers_Id,date_list)

header_df = pd.DataFrame(header_Item_name_list)
data_Frame = {}
data_wining_Numbers_Frame = {}
data_Frame_list =[]
data_wining_Numbers_Frame_list = []

for keys in date_list:
    for i in range(len(header_Item_name_list)):
        data_Frame[header_Item_name_list[i]] =header_Info_Dict[keys][i]
    data_Frame_list.append(dict(data_Frame))  
    for k in range(len(winning_Numbers_title_List)):
        data_wining_Numbers_Frame[header_Item_name_list[0]] = keys
        data_wining_Numbers_Frame[header_Item_name_list[1]] = header_Info_Dict[keys][1]
        data_wining_Numbers_Frame[winning_Numbers_title_List[k]] =  winning_Numbers_Dict[keys][k]
    data_wining_Numbers_Frame_list.append(dict(data_wining_Numbers_Frame))
header_Info_df = pd.DataFrame(data_Frame_list)
winings_Number_df=pd.DataFrame(data_wining_Numbers_Frame_list)
cols = ['期別','開獎日','獎號1', '獎號2', '獎號3', '獎號4', '獎號5', '獎號6','特別號']
winings_Number_df[cols]
insert_to_sql(header_Info_Dict,winning_Numbers_Dict,date_list)