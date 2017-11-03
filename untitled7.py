
with open('E:/sljr/project/5-开发文档/Script/hive/bin/subject_cust_daily.sh', 'rb') as fr:
                for tp in fr.readlines():
                    if('\r\n' in tp):
                        print('ds')
                    print(tp)
                fr.close()