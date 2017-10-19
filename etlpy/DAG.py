# -*- coding:utf-8 -*-
import os
from os.path import getsize, join
import sys
import queue
import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz as pgv 


class DirectorTreeNetworkx():
    """
    用networkx绘制目录结构图
    """

    @classmethod
    def getdirsize(cls, dir):
        """
        获取文件夹大小
        :param dir:
        :return: 返回尺寸
        """
        size= 0

        for root, dirs, files in os.walk(dir):
            size += sum([getsize(join(root, name)) for name in files])

        return size

    @classmethod
    def draw_director_tree(cls, input_path):
        """
        深度遍历一个目录，绘制目录树形图
        :param input_path: 目标目录
        :return:
        """
        if (not os.path.exists(input_path)) or (not os.path.isdir(input_path)):
            print("Input_path Error!")
            return None
        # 用队列做BFS
        director_queue = queue.Queue()
        director_queue.put(input_path)
        # 初始化一个图
        tree_graph = nx.DiGraph()


        tree_graph.add_node(input_path + "\n" + str(os.path.getsize(input_path)))
        while not director_queue.empty():
            new_parent = director_queue.get()
            if os.path.isdir(new_parent):
                child_list = os.listdir(new_parent)
                for child in child_list:
                    full_child = join(new_parent, child)
                    if os.path.isfile(full_child):
                        new_parent_lable = new_parent + "\n" + str(os.path.getsize(new_parent))
                        child_lable = full_child + "\n" + str(os.path.getsize(full_child))
                        tree_graph.add_node(child_lable)
                        tree_graph.add_edge(new_parent_lable, child_lable)
                    elif os.path.isdir(full_child):
                        new_parent_lable = new_parent + "\n" + str(os.path.getsize(new_parent))
                        child_lable = full_child + "\n" + str(os.path.getsize(full_child))
                        tree_graph.add_node(child_lable)
                        tree_graph.add_edge(new_parent_lable, child_lable)
                        director_queue.put(full_child)
        nx.draw(tree_graph)
        plt.show()

class DrawDirectorTree():  
    """ 
    绘制目录结构图，用树的形式 
    """  
 
    @classmethod  
    def getdirsize(cls, dir):  
        """ 
        获取文件夹大小 
        :param dir: 
        :return: 返回尺寸 
        """  
        size = 0 
  
        for root, dirs, files in os.walk(dir):  
            size += sum([getsize(join(root, name)) for name in files])  
  
        return size  
 
    @classmethod  
    def draw_director_tree(cls, input_path):  
        """ 
        深度遍历一个目录，绘制目录树形图 
        :param input_path: 目标目录 
        :return: 
        """  
        if (not os.path.exists(input_path)) or (not os.path.isdir(input_path)):  
            print("Input_path Error!")
            return None  
        # 用队列做BFS  
        director_queue = queue.Queue()  
        director_queue.put(input_path)  
        # 初始化一个图  
        tree_graph = pgv.AGraph(directed=True, strict=True)  
        tree_graph.node_attr['style'] = 'filled'  
        tree_graph.node_attr['shape'] = 'square'  
  
        tree_graph.add_node(input_path + "\n" + str(os.path.getsize(input_path)))  
        while not director_queue.empty():  
            new_parent = director_queue.get()  
            if os.path.isdir(new_parent):  
                child_list = os.listdir(new_parent)  
                for child in child_list:  
                    full_child = join(new_parent, child)  
                    if os.path.isfile(full_child):  
                        new_parent_lable = new_parent + "\n" + str(os.path.getsize(new_parent))  
                        child_lable = full_child + "\n" + str(os.path.getsize(full_child))  
                        tree_graph.add_node(child_lable)  
                        tree_graph.add_edge(new_parent_lable, child_lable)  
                    elif os.path.isdir(full_child):  
                        new_parent_lable = new_parent + "\n" + str(os.path.getsize(new_parent))  
                        child_lable = full_child + "\n" + str(os.path.getsize(full_child))  
                        tree_graph.add_node(child_lable)  
                        tree_graph.add_edge(new_parent_lable, child_lable)  
                        director_queue.put(full_child)  
        tree_graph.graph_attr['epsilon'] = '0.001'  
        #print(tree_graph.string())  # print dot file to standard output  
        tree_graph.write('director_tree.dot')  
        tree_graph.layout('dot')  # layout with dot  
        tree_graph.draw('director_tree.png')  # write to file  
  
  
#DrawDirectorTree.draw_director_tree('/home/aron/workspace/python_space/')         
if __name__=='__main__':
    #DirectorTreeNetworkx.draw_director_tree(input_path='E:\\sljr\\project\\5-开发文档\\Script\\hive')
    DrawDirectorTree.draw_director_tree(input_path='E:\\sljr\\project\\5-开发文档\\Script\\hive')