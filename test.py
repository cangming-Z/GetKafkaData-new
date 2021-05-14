msg = {}


def a():
    tag_detail = {'a': [], 'b': []}
    msg['232@@opc_data'] = tag_detail

a()
print(msg)


site = {'name': '我的博客地址', 'alexa': 10000, 'url':'http://blog.csdn.net/uuihoo/'}
del site['name']
print(site)