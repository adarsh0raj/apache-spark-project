s = """31.56.96.51 - - [22/Jan/2019:03:56:16 +0330] "GET /image/61474/productModel/200x200
HTTP/1.1" 200 5379 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0;
ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko)
Chrome/66.0.3359.158 Mobile Safari/537.36" "-" """

temp = s.split(" ")
res = [temp[0], (temp[3]+temp[4]).lstrip('[').rstrip(']'), temp[5].strip('"'), int(temp[7]), int(temp[8])]
print(res)