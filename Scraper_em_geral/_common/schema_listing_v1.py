# -*- coding: utf-8 -*-
schema_listing_v1 = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "listing_canonical v1",
  "type": "object",
  "required": ["cod_prod","marketplace","listing_id","url","title","price","currency","observed_at"],
  "properties": {
    "cod_prod":{"type":"string","minLength":1},
    "marketplace":{"type":"string","enum":["mercadolivre","magalu","pneustore"]},
    "listing_id":{"type":"string","minLength":1},
    "url":{"type":"string","minLength":1},
    "title":{"type":"string","minLength":1},
    "price":{"type":"number","minimum":0},
    "promo_price":{"type":"number","minimum":0},
    "currency":{"type":"string","pattern":"^[A-Z]{3}$"},
    "seller":{"type":"string"}, "seller_id":{"type":"string"},
    "availability":{"type":"string","enum":["in_stock","out_of_stock","preorder","unknown"]},
    "brand":{"type":"string"}, "model":{"type":"string"},
    "width":{"type":"integer","minimum":50,"maximum":500},
    "aspect":{"type":"integer","minimum":20,"maximum":95},
    "rim":{"type":"integer","minimum":10,"maximum":30},
    "size_norm":{"type":"string"},
    "pack_qty":{"type":"integer","minimum":1}, "is_kit":{"type":"boolean"},
    "size_regex_hit":{"type":"boolean"},
    "observed_at":{"type":"string","format":"date-time"},
    "run_id":{"type":"string"},
    "thumbnail":{"type":"string"}, "extra_images":{"type":"array","items":{"type":"string"}}
  },
  "additionalProperties": True
}
