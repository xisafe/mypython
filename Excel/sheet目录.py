import xlwt
book = xlwt.Workbook()
sheet_index = book.add_sheet('index')
line=0
for i in range(9):
    sheet1 = book.add_sheet(str(i))
    sheet1.write(0,0,str(i))
    link = 'HYPERLINK("#%s!A1";"%s")' % (str(i), str(i))
    sheet_index.write(line, 0, xlwt.Formula(link))
    line+=1
book.save('simple2.xls')