import gc
import json
import logging
from dataclasses import dataclass
from importlib import resources
from typing import TYPE_CHECKING, Optional
import time

if TYPE_CHECKING:
	from playwright.async_api import Page

from browser_use.dom.views import (
	DOMBaseNode,
	DOMElementNode,
	DOMState,
	DOMTextNode,
	SelectorMap,
)
from browser_use.utils import time_execution_async

logger = logging.getLogger(__name__)


@dataclass
class ViewportInfo:
	width: int
	height: int


class DomService:
	def __init__(self, page: 'Page'):
		self.page = page
		self.xpath_cache = {}

		self.js_code = resources.read_text('browser_use.dom', 'buildDomTree.js')

	# region - Clickable elements
	@time_execution_async('--get_clickable_elements')
	async def get_clickable_elements(
		self,
		highlight_elements: bool = True,
		focus_element: int = -1,
		viewport_expansion: int = 0,
	) -> DOMState:
		element_tree, selector_map = await self._build_dom_tree(highlight_elements, focus_element, viewport_expansion)
		return DOMState(element_tree=element_tree, selector_map=selector_map)

	@time_execution_async('--build_dom_tree')
	async def _build_dom_tree(
		self,
		highlight_elements: bool,
		focus_element: int,
		viewport_expansion: int,
	) -> tuple[DOMElementNode, SelectorMap]:
		args = {
			'doHighlightElements': highlight_elements,
			'focusHighlightIndex': focus_element,
			'viewportExpansion': viewport_expansion,
		}
		
		t0 = time.time()
		result = await self.page.evaluate(self.js_code, args)
		
		# Handle the JSON string returned from JavaScript
		try:
			if isinstance(result, str):
				eval_page = json.loads(result)
			else:
				# Backward compatibility with older versions
				eval_page = result
		except json.JSONDecodeError:
			# If JSON parsing fails, assume it's not a string that needs parsing
			eval_page = result
		
		t9 = time.time()
		logger.info(f"buildDomTree.js time: {t9-t0:.3f}(Sec)")
		
		# Use construct_dom_tree for complex JSON structure with node map
		if isinstance(eval_page, dict) and 'map' in eval_page and 'rootId' in eval_page:
			return await self._construct_dom_tree(eval_page)
		
		# Fallback to simple node parsing for direct node data
		if not isinstance(eval_page, dict):
			raise ValueError(f"Expected dict, got {type(eval_page)}: {eval_page}")
		
		html_to_dict = self._parse_node(eval_page)
		
		if html_to_dict is None or not isinstance(html_to_dict, DOMElementNode):
			raise ValueError('Failed to parse HTML to dictionary')
		
		# Create selector map by traversing the tree
		selector_map = {}
		
		def _collect_interactive_elements(node):
			if isinstance(node, DOMElementNode) and node.highlight_index is not None:
				selector_map[node.highlight_index] = node
				
			if isinstance(node, DOMElementNode):
				for child in node.children:
					_collect_interactive_elements(child)
		
		_collect_interactive_elements(html_to_dict)
		
		return html_to_dict, selector_map

	@time_execution_async('--construct_dom_tree')
	async def _construct_dom_tree(
		self,
		eval_page: dict,
	) -> tuple[DOMElementNode, SelectorMap]:
		js_node_map = eval_page['map']
		js_root_id = eval_page['rootId']

		selector_map = {}
		node_map = {}

		# First pass: create all nodes
		for id, node_data in js_node_map.items():
			node = self._parse_node(node_data)
			if node is None:
				continue

			node_map[id] = node

			if isinstance(node, DOMElementNode) and node.highlight_index is not None:
				selector_map[node.highlight_index] = node

		# Second pass: build the tree structure
		for id, node_data in js_node_map.items():
			if id not in node_map:
				continue
			
			# Skip if not an element node or has no children
			if not isinstance(node_map[id], DOMElementNode) or 'children' not in node_data:
				continue
			
			parent_node = node_map[id]
			
			# Process children
			for child_id in node_data['children']:
				if child_id not in node_map:
					continue

				child_node = node_map[child_id]
				child_node.parent = parent_node
				parent_node.children.append(child_node)

		html_to_dict = node_map[str(js_root_id)]

		del node_map
		del js_node_map
		del js_root_id

		gc.collect()

		if html_to_dict is None or not isinstance(html_to_dict, DOMElementNode):
			raise ValueError('Failed to parse HTML to dictionary')

		return html_to_dict, selector_map

	def _parse_node(
		self,
		node_data: dict,
	) -> Optional[DOMBaseNode]:
		if not node_data:
			return None

		# Process text nodes immediately
		if node_data.get('type') == 'TEXT_NODE':
			text_node = DOMTextNode(
				text=node_data['text'],
				is_visible=node_data['isVisible'],
				parent=None,
			)
			return text_node

		# Process coordinates if they exist for element nodes

		viewport_info = None

		if 'viewport' in node_data:
			viewport_info = ViewportInfo(
				width=node_data['viewport']['width'],
				height=node_data['viewport']['height'],
			)

		element_node = DOMElementNode(
			tag_name=node_data['tagName'],
			xpath=node_data['xpath'],
			attributes=node_data.get('attributes', {}),
			children=[],
			is_visible=node_data.get('isVisible', False),
			is_interactive=node_data.get('isInteractive', False),
			is_top_element=node_data.get('isTopElement', False),
			is_in_viewport=node_data.get('isInViewport', False),
			highlight_index=node_data.get('highlightIndex'),
			shadow_root=node_data.get('shadowRoot', False),
			parent=None,
			viewport_info=viewport_info,
		)

		return element_node
