import savReaderWriter as sav
#mydata=sav.SavReader(r'D:\Program Files\IBM\SPSS\statistics\Samples\English\accidents.sav')
#my=sav.SavReader(r'D:\Program Files\IBM\SPSS\statistics\Samples\Simplified Chinese\accidents.sav',ioUtf8=True)
savFileName = r'D:\Program Files\IBM\SPSS\statistics\Samples\English\accidents.sav'
with sav.SavReader(savFileName,returnHeader=True,ioLocale='english') as reader:
    for line in reader:
        print(line)
reader.close()        
with sav.SavReader(savFileName,returnHeader=True,ioLocale='english') as reader:
    records = reader.all()
print(records)
data = sav.SavReader(savFileName) 
print("The first five records look like this: %s" % data.head())
data.close()