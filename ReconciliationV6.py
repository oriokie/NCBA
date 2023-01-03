from tokenize import Ignore
import pandas as pd
import os
import sys
import time
import logging
import datetime
import itertools
import threading
import shutil

pd.options.mode.chained_assignment = None  # default='warn'

done = False

def animate(message="loading", endmessage="Done!"):
    for c in itertools.cycle(["|", "/", "-", "\\"]):
        if done:
            break
        sys.stdout.write(f"\r {message}" + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write(f"\r {endmessage} ")


t = threading.Thread(
    target=lambda: animate(message="Watch this space...", endmessage="Let's Do This!!!")
)
t.start()

time.sleep(3)

done = True

time.sleep(3)

logging.basicConfig(filename = 'file.log',
                    level = logging.DEBUG,
                    format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')
 
logging.info('Starting the bot')


if not os.path.isdir("Reports"):
    os.mkdir("Reports")
if not os.path.isdir("Archive"):
    os.mkdir("Archive")
if not os.path.exists('./STATEMENT'):
    logging.error("Statement File not Found")
if not os.path.exists('./KES.xls'):
     logging.error("KES File not found")
if not os.path.exists('./EFT.xls'):
    logging.error('EFT file not found')
if not os.path.exists('./DD.xls'):
    logging.error('DD File not Found')

logging.info('Checked/Made Directories')

print('...')
print('...')
print('...')
print('...')
print('...')


logging.info('Reading and Formating the statement')

sdf = pd.read_fwf('./STATEMENT',header=None, widths=[13,20,15,9,32,16], index=False)
sortdf = sdf.sort_values(0)
val = sortdf.loc[(sortdf[1] == 'BALANCE AT PERIOD EN'), 4].iloc[0]
df = sortdf.loc [sortdf[5].isin(['KES1020000010001'])]
df.loc[df[4].str.endswith("-"), 4] = (
    "-" + df.loc[df[4].str.endswith("-"), 4].str.strip("- ")
)
pd.to_numeric ([4])

cleandf = df [[1,2,4]]
cleandf.columns = ['NARRATION', 'FT', 'AMOUNT']

cleandf ['AMOUNT'] = cleandf ['AMOUNT'].str.replace(',', '').astype(float)

cleandf = cleandf.sort_values(['AMOUNT'])

print(cleandf.head(2))

postivesum = cleandf[cleandf['AMOUNT']>0]['AMOUNT'].sum()
negativesum = cleandf[cleandf['AMOUNT']<0]['AMOUNT'].sum()

print('Done sanitizing the statement')

logging.info('Reading and Formating the DD Report')

DDrawreport = pd.read_excel("./DD.xls", index_col=False)
DDrawreport [['PROCNO','DESTACCOUNT']] = DDrawreport [['PROCNO','DESTACCOUNT']].astype(str)
DDreport = DDrawreport [(DDrawreport['STATUSID'].isin ([1]) & (~DDrawreport ['DESTBANK'].isin (["NCBA BANK KENYA PLC", "NIC BANK PLC"])))]
DDdf = DDreport [['POLICY1','FTREFERENCE','AMOUNT' ]] 
DDdf.columns = ['POLICY1','FT', 'AMOUNT']
DDdf ['AMOUNT'] = DDdf ['AMOUNT'].astype(float)

DDsum = DDdf ['AMOUNT'].sum()


print ('DD FTs collected')

print (DDdf.head(2))

logging.info('Reading and Formating the EFT Report')

EFTdata = pd.read_excel("./EFT.xls", index_col=False)
EFTdata [['PROCNO','DESTACCOUNT']] = EFTdata [['PROCNO','DESTACCOUNT']].astype(str)
EFTdf = EFTdata [['ACHBULKID','TRNREF','AMOUNT' ]]
EFTdf.columns = ['ACHBULKID','FT', 'AMOUNT']
#EFTdf ['AMOUNT'] = EFTdf ['AMOUNT'].astype(float)
EFTsum = EFTdf ['AMOUNT'].sum()

print ('EFT FTs Collected')
print (EFTdf.head(2))

logging.info('Reading and Formating the CHQs Report')

CHQraw = pd.read_excel("./KES.xls", index_col=False)
CHQraw [['PROCNO','DESTACCOUNT','CHEQUENO']] = CHQraw [['PROCNO','DESTACCOUNT','CHEQUENO']].astype(str)

if 'STATUSID' in CHQraw:
    print('STATUSID FOUND')
    CHQs = CHQraw [(CHQraw['STATUSID'].isin ([1]) & (~CHQraw ['DESTBANK'].isin (["NCBA BANK KENYA PLC", "NIC BANK PLC"])) & (CHQraw ['STAGE'].isin (["ACH CREATION"])))]
else:
    print('NO EXCLUDED CHEQUES FOUND')
    CHQs = CHQraw [(~CHQraw ['DESTBANK'].isin (["NCBA BANK KENYA PLC", "NIC BANK PLC"])) & (CHQraw ['STAGE'].isin (["ACH CREATION"]))]

if CHQs['CBS_REJECT_REASON'].str.contains('NOCREDIT').any():
    print('CREDIT-DUPLICATE VALUES')
    CHQdf = CHQs [['CHEQUENO','CBS_REJECT_REASON','AMOUNT' ]]
    CHQdf[['FT','FT1', 'FT2']] = CHQdf.CBS_REJECT_REASON.str.split ('[,-]', expand =True)
    CHQclr = CHQdf [['CHEQUENO','FT1', 'AMOUNT']]
    CHQclr.columns = ['CHEQUENO','FT', 'AMOUNT']
else:
    CHQdf = CHQs [['CHEQUENO','CBS_REJECT_REASON','AMOUNT' ]]
    CHQdf[['FT','FT1']] = CHQdf.CBS_REJECT_REASON.str.split ('[,]', expand =True)
    CHQclr = CHQdf [['CHEQUENO','FT1', 'AMOUNT']]
    CHQclr.columns = ['CHEQUENO','FT', 'AMOUNT']
    print('NO DUPLICATE ENTRIES')

CHQsum = CHQclr ['AMOUNT'].sum()

print(CHQclr.head(2))

print ('Let Reconciliation Begin')

frames = [DDdf, EFTdf, CHQclr]

allcleared = pd.concat (frames)

print ('Starting Recon')

logging.info('Starting Recon')


left_join = pd.merge (
    cleandf,
    allcleared,
    on='FT',
    how='left'
)

T24E = left_join [(~left_join['AMOUNT_y'].notnull())]

left_join2 = pd.merge (
    allcleared,
    cleandf,
    on='FT',
    how='left'
)

CPE = left_join2 [(~left_join2['AMOUNT_y'].notnull())]

logging.info('Done with recon')

print ('Generating the Recon File')

totaldebits = DDsum + CHQsum
summarydata = [['TOTAL STATEMENT CREDITS', postivesum],['TOTAL STATEMENT DEBITS', negativesum], ['DIRECT DEBITS', DDsum], ['CHEQUES', CHQsum], ['EFTs', EFTsum], ['TOTAL DEBITS CLEARED', totaldebits],['BALANCE AT THE END', val]]
summarydf = pd.DataFrame (summarydata, columns=['DESCRIPTION', 'AMOUNT'])

print (summarydf)

time.sleep(3)

reversals = cleandf[cleandf.duplicated(['FT'], keep=False)]
reversals2 = allcleared[(allcleared.duplicated(['FT'], keep=False)) & (allcleared['FT'].str.contains('FT'))]


CHQsDf = CHQs.applymap(lambda x: x.encode('unicode_escape'). #Basically, it escapes the unicode characters if they exist
                 decode('utf-8') if isinstance(x, str) else x)

amountcheck = left_join [(left_join['AMOUNT_y'].notnull())]
amountcheck ['Diff'] = amountcheck['AMOUNT_y'] - abs(amountcheck['AMOUNT_x'])

WorryDiff = amountcheck.loc[(abs(amountcheck.Diff)) >0.5 ]


output_path = os.path.join ('Reports',time.strftime("RECON%Y-%m-%d.xlsx"))
with pd.ExcelWriter(output_path) as writer:
    cleandf.to_excel(writer, sheet_name='Statement', index=False)
    print("Copied the Statement Entries")
    allcleared.to_excel(writer, sheet_name='Cleared', index=False)
    print("Copied the CP Cleared Entries")
    T24E.to_excel(writer,sheet_name='T24 Exceptions', index=False)
    print("Created T24 Exceptions")
    CPE.to_excel(writer,sheet_name='CP Exceptions', index=False)
    print("Created Chequepoint Exceptions")
    summarydf.to_excel(writer,sheet_name='Summary', index=False)
    print("Created the Summary Sheet")
    DDreport.to_excel(writer,sheet_name='DDS', index=False)
    print("Copied Cleared DDs")
    EFTdata.to_excel(writer,sheet_name='EFTs', index=False)
    print("Copied Cleared EFTs")
    CHQsDf.to_excel(writer, sheet_name='CHQs', index=False)
    print("Copied Cleared CHQs")
    reversals.to_excel(writer, sheet_name='REVERSALS FROM LIVE', index=False)
    print("Reversals Detected...")
    WorryDiff.to_excel(writer, sheet_name='AMOUNT_CHECK', index=False)
    reversals2.to_excel(writer, sheet_name='CLEARED DUPLICATE', index=False)
  
print("Done with the reconciliation. You are Welcome. Edwin")
logging.info('Done with writing files')

logging.info('Archiving Data')

print ('Archiving Files...')
now = str(datetime.datetime.now())[:19]
now = now.replace(":","_")
logging.info('Listing Files Post processing')
src_dir1="./KES.xls"
src_dir2="./DD.xls"
src_dir3="./EFT.xls"
src_dir4="./STATEMENT"
dst_dir1="./Archive/"+str(now)+"KES.xls"
dst_dir2="./Archive/"+str(now)+"DD.xls"
dst_dir3="./Archive/"+str(now)+"EFT.xls"
dst_dir4="./Archive/"+str(now)+"STATEMENT"
logging.info('Copying Files Post processing')
shutil.copy(src_dir1,dst_dir1)
shutil.copy(src_dir2,dst_dir2)
shutil.copy(src_dir3,dst_dir3)
shutil.copy(src_dir4,dst_dir4)

print ('Done')

logging.info('The Bot has finished running')

logging.shutdown()

os.replace("./file.log", time.strftime("./Archive/%Y%m%d%H%M%SFile.log"))

time.sleep(5)