import asyncio
from typing import List, Dict, Any, Optional
from .state import GiftContext, GraphState, MessageRow
from .nodes.init_node import init_node
from .nodes.parent_score_node import parent_score_node
from .nodes.child_score_node import child_score_node
from .nodes.hierarchy_node import hierarchy_node
from .nodes.select_top3_node import select_top3_node
from .nodes.db_filter_node import db_filter_node
from .nodes.product_node import product_node
from .nodes.pack_node import pack_node

async def run_pipeline(rows: List[Dict[str, Any]], ctx: GiftContext, products_csv: Optional[str] = None) -> Dict[str, Any]:
    print(f"🔍 Pipeline 입력 rows: {rows[0] if rows else 'None'}")
    
    row_objs = []
    for i, r in enumerate(rows):
        try:
            row_obj = MessageRow(**r)
            row_objs.append(row_obj)
            if i == 0:
                print(f"🔍 첫 번째 MessageRow: idx={row_obj.idx}, date={row_obj.date}, user={row_obj.user}")
        except Exception as e:
            print(f"❌ MessageRow 생성 실패 (행 {i}): {e}")
            print(f"   데이터: {r}")
            raise
    
    state = GraphState(rows=row_objs, ctx=ctx)

    state = init_node(state)
    state = await parent_score_node(state)
    state = await child_score_node(state)
    state = hierarchy_node(state)
    state = select_top3_node(state)
    state = await db_filter_node(state, products_csv=products_csv)
    state = await product_node(state)
    state = pack_node(state)
    return state.debug["final_payload"]
