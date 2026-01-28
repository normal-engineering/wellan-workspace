rename_dict = {
  '訂單編號' : 'sub',
  '訂購客戶' : 'customer',
  '負責業務員' : 'creator',
  '接單單位' : 'dept_sales',
  '開單日期' : 'date_created',
  '理貨單位' : 'dept_fulfillment',
  '出貨單位' : 'dept_shipping',
  '配送方式' : 'transport',
  '溫層' : 'thermo',
  '送貨日期' : 'date_delivery',
  '送貨時間' : 'time_delivery',
  '送貨地址' : 'address',
  '收貨人' : 'recipient',
  '連絡電話(日)' : 'telephone_day',
  '收貨人手機' : 'mobile',
  '送貨備註一' : 'comment_1',
  '送貨備註二' : 'comment_2',
  '送貨備註三' : 'comment_3',
  '是否結案' : 'status',
  '明細項次' : 'item_index',
  '品牌' : 'brand',
  '產品編號' : 'sku',
  '產品名稱' : 'product',
  '訂購量' : 'qty',
  '商品備註' : 'comment',
  '自組產品編號' : 'custom_sku',
  '自組產品名稱' : 'custom_product',
  '明細數量' : 'custom_qty',
}

from pydantic import TypeAdapter
from typing import TypedDict, Optional, Any, List
from datetime import datetime

class Address(TypedDict):
    Search: str
    PostNumber: str
