# Conversion by Huihao JING on October 19, 2021
# Goal:
   # - Read in boardex data
   # - identify Boardex firms as institutional and/or portfolio firms

# For portoflio firms, we want to match firms to a gvkey 
   # Compustat_2_distinct.xlsx will have the CIKs for portfolio firms 
   # use crosswalks from previous papers - bdx_comp_for_Eyub_LiuJCF and bdx_crsp_comp_link_22jan09_RFS.sas7bdat
   # Combine these 3 datasets to produce a CIK-BoardExID crosswalk
# For institutional investors, merge 13DG_MGRNAME_CIK.csv with distinct_mgrno_mgrname
   # to get institutional investors with associated CIK numbers
import numpy as np
import os
import datetime
import pandas as pd
start = datetime.datetime.now()
ddir = os.path.abspath("\home\ysun9\OpenSecrets")
rdir = os.path.join(ddir,"raw_data")
gdir = os.path.join(ddir,"generated_data")
idir = os.path.join(ddir,"intermediate")
# ==================================================================
# Helper function
# ==================================================================
def anti_join(x, y, on):
    return pd.merge(left=x, right=y, how='left', indicator=True, on=on).query("_merge == 'left_only'").drop(columns='_merge')
def anti_join_all_cols(x, y):
    assert set(x.columns.values) == set(y.columns.values)
    return anti_join(x, y, x.columns.tolist())
# ==================================================================
# Read in BoardEx data and join together 
# ==================================================================
# BoardEx employment profiles. Got some weird encoding issues that I am 
# hacking around
#      boardex_empl = pd.DataFrame(pd.read_csv(rdir+r"BoardEx\DIR_EMPLOYMENT_PROFILE.csv"))
boardex_empl = pd.DataFrame(pd.read_csv("DIR_EMPLOYMENT_PROFILE.csv"))
boardex_empl.loc[:,"DirectorName"] = boardex_empl["DirectorName"].str.replace('[^ -~]', '')
# BoardEx-cik links - from the Boardex dataset itself
#        boardex_cik = pd.DataFrame(pd.read_csv(rdir+r"BoardEx\CIK_BOARDID_ALL.csv"))
boardex_cik = pd.DataFrame(pd.read_csv("CIK_BOARDID_ALL.csv"))
# If BoardEx was missing cik, RA's manually linked the remaining
#       manual_match = pd.DataFrame(pd.read_excel(idir+r"BoardExManualMatch.xlsx"))
manual_match = pd.DataFrame(pd.read_excel("BoardExManualMatch.xlsx"))
# Join the three datasets together to create a master boardex-cik linked dataset
boardex_cik_all = pd.DataFrame(pd.concat([boardex_cik, manual_match]))
boardex_cik_all.loc[:,"CIKCode"] = boardex_cik_all['CIKCode'].str.replace(r'[^\w\s]+', '')
boardex_cik_all.loc[:,"CIKCode"] = boardex_cik_all['CIKCode'].str.strip()
boardex_cik_all.loc[:,"CIKCode"] = boardex_cik_all['CIKCode'].astype('int32')
# ==================================================================
# Read in list of institutional and portfolio firms
# ==================================================================
#      instit = pd.DataFrame(pd.read_excel(idir+r"distinct_mgrno_mgrname.xlsx"))
instit = pd.DataFrame(pd.read_excel("distinct_mgrno_mgrname.xlsx"))
instit.loc[:,'Firm'] = instit['mgrname'].str.upper()
instit_13dg = pd.read_csv(idir+r"13DG_MGRNAME_CIK.csv")
instit_13dg.loc[:,'Firm'] = instit_13dg['fil_cname'].str.upper()
instit_13dg.rename(columns={'cik':'fil_cik'})
# join together the two datasets, taking out anything in instit that appears in 
# instit_13dg to deduplicate
# if there are multiple firms associated with the same CIK number, take the firm
# with the longest string name
'''instit_all <- 
   anti_join(instit, instit_13dg) %>%
   anti_join(instit_13dg) %>%
   bind_rows(instit_13dg) %>%
   filter(!is.na(cik)) %>%
   group_by(Firm) %>%
   arrange(-str_length(Firm)) %>%
   filter(row_number() == 1) %>%
   ungroup'''
foo = anti_join(instit, instit_13dg)
foo = anti_join(foo, instit_13dg)
instit_all = pd.concat([foo, instit_13dg])
instit_all = instit_all[instit_all["cik"].notna(),:]
instit_all = instit_all.sort_values(len(['Firm']),accendin=False)
instit_all = instit_all[1,]
# Portfolio firms --------------------------
# boardex-gvkey dataset1 doesn't have firm names - load in compustat dataset to 
#link gvkey to firm names
#      compustat = pd.DataFrame(pd.read_excel(idir+r"Compustat_2_distinct.xlsx"))
compustat = pd.DataFrame(pd.read_excel("Compustat_2_distinct.xlsx"))
compustat.loc[:,'gvkey'] = compustat['gvkey'].astype('int32')
compustat.rename(columns={'Firm':'conm','Firm_modified':'modified_conm'})
compustat.drop(columns=['delete'])
#        port_boardex1 = pd.read_stata(idir+r"bdx_comp_for_Eyub_LiuJCF.dta")
port_boardex1 = pd.read_stata("bdx_comp_for_Eyub_LiuJCF.dta")
port_boardex1.rename(columns={'BoardID':'boardid'})
port_boardex1 = pd.merge(port_boardex1,compustat,how='left')
'''port_boardex2 <-
   file.path(idir, "bdx_crsp_comp_link_22jan09_RFS.sas7bdat") %>%
   read_sas() %>% 
   select(gvkey, BoardID = CompanyID, Firm = BoardExCompanyName) %>%
   mutate_at(vars(gvkey, BoardID), as.numeric) %>%
   anti_join(select(port_boardex1, BoardID))'''
#     port_boardex2 = pd.DataFrame(pd.read_sas(idir+r"bdx_crsp_comp_link_22jan09_RFS.sas7bdat"))
port_boardex2 = pd.DataFrame(pd.read_sas("bdx_crsp_comp_link_22jan09_RFS.sas7bdat"))
port_boardex2.loc[:,'BoardID'] = port_boardex2['CompanyID']
port_boardex2.loc[:,'Firm'] = port_boardex2['BoardExCompanyName']
port_boardex2 = pd.DataFrame(port_boardex2.columns['gvkey','BoardID','Firm'])
port_boardex2.loc[:,'gvkey'] = port_boardex2['gvkey'].astype['int32']
port_boardex2.loc[:,'BoardID'] = port_boardex2['BoardID'].astype['int32']
port_boardex2 = anti_join(port_boardex2,port_boardex1['BoardID'])
boardex_port = pd.concat([port_boardex1, port_boardex2])
boardex_port = boardex_port[['CompanyID'] == ['BoardID'],['gvkey'],['Firm']]
boardex_port['Portfolio'] = True
# ==================================================================
# Identify institutional and portfolio firms in employment profiles
# ==================================================================
# join boardex to institutional firms by cikcode 
'''boardex_instit <-
   boardex_cik_all %>%
   filter(!is.na(CIKCode)) %>%
   inner_join(select(instit_all, CIKCode = cik)) %>%
   mutate(Institutional = TRUE) %>%
   distinct() %>%
   mutate(delisted = str_detect(BoardName, regex("De-listed", ignore_case = TRUE)))  %>%
   group_by(CIKCode) %>%
   mutate(n = n()) %>%
   filter(!(delisted & n > 1)) %>%
   ungroup %>%
   select(CompanyID = BoardID, CIKCode, Institutional, Firm = BoardName)
'''
boardex_instit = boardex_cik_all[boardex_cik_all["CIKCode"].notna()]
boardex_instit = pd.merge(boardex_instit,instit_all[instit_all["CIKCode"] == instit_all["cik"]])
boardex_instit['Institutional'] = True
boardex_instit.drop_duplicates()
boardex_instit.loc['delisted'] = boardex_instit['BoardName'].str.contains("De-listed", case=False, flags=0, na=None, regex=True)
boardex_instit = (boardex_instit.groupby(['CIKCode']).agg({'CIKCode':'size'}).reset_index())
boardex_instit = boardex_instit[boardex_instit['n']<=1 ]
boardex_instit.loc[:,'CompanyID'] = boardex_instit['BoardID']
boardex_instit.loc[:,'Firm'] = boardex_instit['BoardName']
boardex_instit = pd.DataFrame(boardex_instit.columns['CompanyID','CIKCode','Institutional','Firm'])
firms_all = pd.merge(boardex_instit,boardex_port,how='outer')
'''boardex_df <-
   boardex_empl %>%
   left_join(firms_all) %>%
   mutate_at(vars(Institutional, Portfolio), ~replace_na(.x, FALSE)) %>%
   mutate(DateStartRole = ymd(DateStartRole), 
          DateEndRole = ymd(DateEndRole))'''
boardex_df = pd.DataFrame(pd.merge(boardex_empl,firms_all,on='Product_ID',how='left'))
boardex_df.loc[:,'Institutional'] = boardex_df['gvkey'].fillna(inplace = False)
boardex_df.loc[:,'Portfolio'] = boardex_df['BoardID'].fillna(inplace = False)
boardex_df['DateStartRole'] = pd.to_datetime(boardex_df['DateStartRole'])
boardex_df['DateEndRole'] = pd.to_datetime(boardex_df['DateEndRole'])
#      boardex_df.to_csv(gdir+r"BoardEx/boardex_empl.csv")
boardex_df.to_csv()
end = datetime.datetime.now()
print("Boardex Import Done In: ", pd.to_datetime(end) - pd.to_datetime(start))
