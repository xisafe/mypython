from PIL import Image,ImageFilter
 
Diff_radius = 500
diff_min = 1
r_h, g_h, b_h = 43, 55, 66
image = Image.open(r'c:\test.png')
image_width = image.size[0]
image_height = image.size[1]
rgb_im = image.convert('RGB')
 
img_sharpen = image.filter(ImageFilter.SHARPEN)
 
img_new = Image.new('RGBA', image.size, (255,255,255,255))
img_copy = img_sharpen.crop((0,0,image_width,image_height))
img_new.paste(img_copy, (0,0,image_width,image_height))
 
y_tmp = 0
print "ot"
for y in range(image_height):
    y_is_black = 0
    current_line_flag_acc = 0
    for x in range(image_width):
        r, g, b = rgb_im.getpixel((x, y))
        if ((r_h-r)**2 + (g_h-g)**2 + (b_h-b)**2) < Diff_radius :
            current_line_flag_acc = current_line_flag_acc + 1
            if (x == image_width - 1) and (current_line_flag_acc > diff_min) :
                y_is_black = 1
                img1 = img_new.crop((0,y_tmp-2,image_width,y_tmp-1))
                img_new.paste(img1, (0,y,image_width,y+1))
                # print('y_tmp:%d -> y:%d'%(y_tmp,y))
        elif (x == image_width - 1) and not y_is_black:
            y_tmp = y
# img_save = img_new.filter(ImageFilter.GaussianBlur(radius=1.5))
img_new.save(r'c:\test_1.png')
print("done!")