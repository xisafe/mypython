import sqlite3
from PIL import Image, ImageDraw
#http://www.cnblogs.com/paiandlu/articles/6985916.html
def sampledata():
    db=sqlite3.connect('dbname.db')
    cur=db.cursor()
    cur.execute("create table if not exists relation(mother, child)");
    cur.execute("INSERT INTO relation(mother, child) VALUES('1000', '1100')");
    cur.execute("INSERT INTO relation(mother, child) VALUES('1000', '1200')");
    cur.execute("INSERT INTO relation(mother, child) VALUES('1000', '1300')");
    cur.execute("INSERT INTO relation(mother, child) VALUES('1000', '1400')");

    cur.execute("INSERT INTO relation(mother, child) VALUES('1200', '1210')");
    cur.execute("INSERT INTO relation(mother, child) VALUES('1200', '1220')");
    cur.execute("INSERT INTO relation(mother, child) VALUES('1200', '1230')");

    cur.execute("INSERT INTO relation(mother, child) VALUES('1220', '1221')");
    cur.execute("INSERT INTO relation(mother, child) VALUES('1220', '1222')");

    db.commit();
class decisionnode:
    def __init__(self, value, isleaf=False, leafcounts=0, maxlevel=1):
        self.childs=[]
        self.value=value
        self.isleaf=isleaf
        self.leafcounts=leafcounts
        self.maxlevel=maxlevel
    
    def addchild(self, child):
        self.childs.append(child)

class RelationTree:
    def __init__(self, basewidth=100, basedepth=100):
        self.basewidth = basewidth
        self.basedepth = basedepth
        self.root=None
        
    def gentree(self, db, mothervalue, tablename, childcol, mothercol):
        self.root=decisionnode(mothervalue)
        cur=db.cursor()
        def swap_gentree(node):
            cur.execute("select %s from %s where %s = '%s'" % \
                (childcol, tablename, mothercol, node.value));
            results=cur.fetchall()
            
            #如果是叶子节点，则直接返回
            if not results:
                return decisionnode(node.value, isleaf=True, maxlevel=1)
            
            #程序运行到这里，说明是非叶子节点
            #对非叶子节点进行其下包含的叶子节点进行统计（leafcounts）
            #该节点之下最深的深度maxlevel收集
            maxlevel=1
            for each in results:
                entrynode=swap_gentree(decisionnode(each[0]))
                if(entrynode.isleaf):
                    node.leafcounts += 1
                else:
                    node.leafcounts += entrynode.leafcounts
                
                if (entrynode.maxlevel > maxlevel):
                    maxlevel = entrynode.maxlevel
                node.addchild(entrynode)
            
            node.maxlevel = maxlevel+1
            return node
        swap_gentree(self.root)



    def draweachnode(self, tree, draw, x, y):
        draw.text((x,y), tree.value, (0,0,0))
        
        if not tree.childs:
            return
        
        childs_leafcounts=[child.leafcounts if child.leafcounts else 1 for child in tree.childs]

        leafpoint=x-sum(childs_leafcounts)*self.basewidth/2

        cumpoint=0
        for childtree, point in zip(tree.childs, childs_leafcounts):
            centerpoint=leafpoint+self.basewidth*cumpoint+self.basewidth*point/2
            cumpoint += point
            draw.line((x,y, centerpoint, y+self.basedepth), (255,0,0))
            self.draweachnode(childtree, draw, centerpoint, y+self.basedepth)
            

    def drawTree(self, filename='tree.jpg'):
        width=self.root.leafcounts * self.basewidth + self.basewidth
        depth=self.root.maxlevel * self.basedepth + self.basedepth
        img=Image.new(mode="RGB", size=(width, depth), color=(255,255,255))
        draw=ImageDraw.Draw(img)
        self.draweachnode(self.root, draw, width/2, 20)
        
        img.save(filename)
if __name__ == '__main__':
    #sampledata()
    db=sqlite3.connect('dbname.db')
    tree=RelationTree()
    tree.gentree(db,'1000','relation','child','mother')
    tree.drawTree(filename='d:/tree.jpg')
        