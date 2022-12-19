import pandas as pd
import os
import time
import datetime
import timeit

starttime = timeit.default_timer()
print("The start time is :",starttime)

status_values = ['02','04','03','06','04','01']
if not os.path.isdir("Reports"):
    os.mkdir("Reports")


alldata = pd.read_excel("./Originator.xls", index_col=False)

data = alldata[['Unnamed: 25','Unnamed: 24','Unnamed: 17','Unnamed: 13','Unnamed: 8','Unnamed: 35','Unnamed: 3','Unnamed: 2','Unnamed: 1']]
Cdata = data[data['Unnamed: 2'].isin(status_values) ==True]
Cdata.columns = ['POLICY1', 'ORGCODE','ACCOUNT_NO_CREDIT','ACCOUNT_NO_DEBIT','BANK','PAYERS_NAME','AMOUNT_TO','STATUS','PROCNO']

Cancelled_CHI = Cdata.loc[(Cdata['STATUS'].isin(['06'])),['POLICY1', 'ORGCODE','ACCOUNT_NO_CREDIT','ACCOUNT_NO_DEBIT','BANK','PAYERS_NAME','AMOUNT_TO','STATUS','PROCNO']]
NotCancelled_CHI = Cdata.loc[(~Cdata['STATUS'].isin(['06'])),['POLICY1', 'ORGCODE','ACCOUNT_NO_CREDIT','ACCOUNT_NO_DEBIT','BANK','PAYERS_NAME','AMOUNT_TO','STATUS','PROCNO']]

NotCancelled_CHI.head(2)

master = pd.read_excel ("./MASTER.xls", index_col=False)
master['ORGPROCNO'] = master['ORGPROCNO'].fillna(master['PROCNO'])
CPMaster = master[~master['REMARKS'].isin(['Incoming Mandates'])]
CleanCP =  CPMaster[(master['STATUSDESC'].isin(['APPROVED'])) & (~master['DEBIT_BANK_NAME'].isin(['NIC BANK PLC','NCBA BANK KENYA PLC']))]

CPMaster = master [['POLICY1','ORGCODE','ACCOUNTNAME','ACCOUNT_NO_DEBIT','PAYERS_NAME','DEBIT_BANK_NAME','DEBIT_BRANCH_NAME','PROCNO','AMOUNT_TO','ACCOUNT_NO_CREDIT','ACCOUNT_CREDIT_NAME','ORGPROCNO','FREQUENCY','STATUSDESC']]
CleanCP = CleanCP [['POLICY1','ORGCODE','ACCOUNT_NO_DEBIT','PAYERS_NAME','DEBIT_BANK_NAME','DEBIT_BRANCH_NAME','PROCNO','AMOUNT_TO','ACCOUNT_NO_CREDIT','ORGPROCNO','FREQUENCY','STATUSDESC']]
CleanCP.head(2)

left_join = pd.merge(
    CleanCP,
    Cdata,
    on=['POLICY1'],
    how='left',
)
left_join ['ComparePROC'] = left_join['ORGPROCNO'] == left_join['PROCNO_y']
left_join ['ComparePROC']


chi_join = pd.merge(
    Cdata,
    CleanCP,
    on=['POLICY1'],
    how='left',
)

cancel_check = pd.merge(
    CleanCP,
    Cancelled_CHI,
    on=['POLICY1'],
    how='left',
)

left_join['Count'] = left_join.groupby(['POLICY1'], sort=False).cumcount() + 1
NotCHI = left_join.loc[(left_join['PROCNO_y'].isnull()),['POLICY1','ORGCODE_x','ACCOUNT_NO_DEBIT_x','PAYERS_NAME_x','DEBIT_BANK_NAME','PROCNO_x','AMOUNT_TO_x','ACCOUNT_NO_CREDIT_x','ORGPROCNO']]
Approved_Cancelled = cancel_check.loc[(~cancel_check['PROCNO_y'].isnull())]

Approved_Cancelled.head(1)

cancel_check2 = pd.merge(
    Approved_Cancelled,
    NotCancelled_CHI,
    on=['POLICY1'],
    how='left',
)

Cancelled_Cancelled = cancel_check2.loc[(cancel_check2['STATUS_y'].isnull())]
Cancelled_Cancelled

output_path = os.path.join ('Reports',time.strftime("DDMANDATES%Y-%m-%d.xlsx"))
with pd.ExcelWriter(output_path) as writer:
    CPMaster.to_excel(writer, sheet_name='CP MASTER', index=False)
    Cdata.to_excel(writer, sheet_name='CHI', index=False)
    left_join.to_excel(writer, sheet_name='CP-CHI', index=False)
    chi_join.to_excel(writer, sheet_name='CHI-CP', index=False)
    NotCHI.to_excel(writer,sheet_name='NOT IN CHI', index=False)
    Cancelled_Cancelled.to_excel(writer,sheet_name='CANCELLED IN CHI', index=False)

print("Seconds Taken :", timeit.default_timer() - starttime)