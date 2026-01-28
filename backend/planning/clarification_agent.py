"""
Proactive Clarification Agent

Detects ambiguous queries and asks intelligent clarifying questions BEFORE making assumptions.
This is the key difference from fail-open mode - we ask rather than guess.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple
import json
import logging
import time

logger = logging.getLogger(__name__)


@dataclass
class ClarificationQuestion:
    """A clarification question to ask the user."""
    question: str
    context: str  # Why this question is needed
    field: str  # What field/parameter needs clarification
    options: Optional[List[str]] = None  # Optional choices for the user
    required: bool = True  # Whether this clarification is required
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'question': self.question,
            'context': self.context,
            'field': self.field,
            'required': self.required
        }
        if self.options:
            result['options'] = self.options
        return result


@dataclass
class ClarificationResult:
    """Result of clarification analysis."""
    needs_clarification: bool
    questions: List[ClarificationQuestion]
    confidence: float  # Confidence in current interpretation (0-1)
    suggested_intent: Optional[Dict[str, Any]] = None  # Best guess if we proceed
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'needs_clarification': self.needs_clarification,
            'questions': [q.to_dict() for q in self.questions],
            'confidence': self.confidence
        }
        if self.suggested_intent:
            result['suggested_intent'] = self.suggested_intent
        return result


class ClarificationAgent:
    """
    Proactive clarification agent that detects ambiguities and asks questions.
    
    This is different from fail-open mode:
    - Fail-open: Makes assumptions and warns
    - Clarification: Asks questions BEFORE proceeding
    """
    
    def __init__(self, llm_provider=None, metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize clarification agent.
        
        Args:
            llm_provider: Optional LLM provider for generating questions
            metadata: Optional metadata for context-aware questions
        """
        self.llm_provider = llm_provider
        self.metadata = metadata or {}
    
    def analyze_query(self, query: str, intent: Optional[Dict[str, Any]] = None,
                     metadata: Optional[Dict[str, Any]] = None) -> ClarificationResult:
        """
        Analyze query for ambiguities and generate clarification questions.
        
        Args:
            query: User query text
            intent: Optional extracted intent (if already extracted)
            metadata: Optional metadata for context
        
        Returns:
            ClarificationResult with questions if needed
        """
        metadata = metadata or self.metadata
        
        # Track timing for metrics
        start_time = time.time()
        
        # Detect ambiguities
        ambiguities = self._detect_ambiguities(query, intent, metadata)
        
        if not ambiguities:
            time_ms = (time.time() - start_time) * 1000
            
            # Log event
            try:
                from backend.planning.clarification_metrics import log_clarification_event
                log_clarification_event(
                    'query_analyzed',
                    needs_clarification=False,
                    confidence=1.0,
                    questions_count=0,
                    time_ms=time_ms
                )
            except ImportError:
                pass
            
            return ClarificationResult(
                needs_clarification=False,
                questions=[],
                confidence=1.0,
                suggested_intent=intent
            )
        
        # Generate clarification questions
        questions = self._generate_questions(ambiguities, query, metadata)
        
        # Calculate confidence (lower if many ambiguities)
        confidence = max(0.0, 1.0 - (len(ambiguities) * 0.2))
        
        time_ms = (time.time() - start_time) * 1000
        
        # Log event
        try:
            from backend.planning.clarification_metrics import log_clarification_event
            log_clarification_event(
                'query_analyzed',
                needs_clarification=True,
                confidence=confidence,
                questions_count=len(questions),
                time_ms=time_ms,
                ambiguities=[a.get('type') for a in ambiguities]
            )
        except ImportError:
            pass
        
        return ClarificationResult(
            needs_clarification=True,
            questions=questions,
            confidence=confidence,
            suggested_intent=intent  # Best guess if user proceeds anyway
        )
    
    def _detect_ambiguities(self, query: str, intent: Optional[Dict[str, Any]],
                           metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect ambiguities in the query.
        
        Returns:
            List of ambiguity dictionaries with type, field, context
        """
        ambiguities = []
        query_lower = query.lower()
        
        # 1. Check for missing metric (for metric queries)
        if intent:
            query_type = intent.get('query_type', 'relational')
            metric = intent.get('metric')
            
            # Check if query seems like a metric query but no metric specified
            metric_keywords = ['total', 'sum', 'count', 'average', 'aggregate', 'revenue', 'sales']
            has_metric_keyword = any(kw in query_lower for kw in metric_keywords)
            
            if query_type == 'metric' and not metric and has_metric_keyword:
                ambiguities.append({
                    'type': 'missing_metric',
                    'field': 'metric',
                    'context': 'Query mentions aggregation but no specific metric identified',
                    'query_hints': [kw for kw in metric_keywords if kw in query_lower]
                })
        
        # 2. Check for ambiguous table selection
        if intent:
            base_table = intent.get('base_table')
            if not base_table:
                # Try to infer from query
                tables = metadata.get('tables', {}).get('tables', [])
                matching_tables = []
                
                for table in tables:
                    table_name = table.get('name', '').lower()
                    table_desc = table.get('description', '').lower()
                    
                    # Check if query keywords match table name or description
                    query_words = set(query_lower.split())
                    table_words = set(table_name.split('_') + table_desc.split())
                    
                    if query_words.intersection(table_words):
                        matching_tables.append(table.get('name'))
                
                if len(matching_tables) > 1:
                    ambiguities.append({
                        'type': 'ambiguous_table',
                        'field': 'base_table',
                        'context': f'Multiple tables match query: {", ".join(matching_tables)}',
                        'options': matching_tables
                    })
                elif len(matching_tables) == 0:
                    ambiguities.append({
                        'type': 'missing_table',
                        'field': 'base_table',
                        'context': 'No table identified from query',
                        'available_tables': [t.get('name') for t in tables[:10]]  # Limit to 10
                    })
        
        # 3. Check for missing time range (for time-series queries)
        if intent:
            query_type = intent.get('query_type', 'relational')
            time_range = intent.get('time_range')
            time_context = intent.get('time_context')
            
            time_keywords = ['last', 'recent', 'today', 'yesterday', 'week', 'month', 'year']
            has_time_keyword = any(kw in query_lower for kw in time_keywords)
            
            if query_type == 'metric' and not time_range and not time_context and not has_time_keyword:
                ambiguities.append({
                    'type': 'missing_time_range',
                    'field': 'time_range',
                    'context': 'Metric query but no time range specified',
                    'suggestions': ['last 7 days', 'last 30 days', 'last 90 days', 'all time']
                })
        
        # 4. Check for ambiguous dimensions
        if intent:
            dimensions = intent.get('dimensions', [])
            group_by = intent.get('group_by', [])
            
            # Check if query mentions grouping but no dimensions specified
            group_keywords = ['by', 'group', 'grouped', 'per', 'for each']
            has_group_keyword = any(kw in query_lower for kw in group_keywords)
            
            if query_type == 'metric' and has_group_keyword and not dimensions and not group_by:
                # Try to find potential dimensions
                available_dimensions = []
                semantic_registry = metadata.get('semantic_registry', {})
                for dim in semantic_registry.get('dimensions', []):
                    dim_name = dim.get('name', '').lower()
                    if any(kw in dim_name for kw in query_lower.split()):
                        available_dimensions.append(dim.get('name'))
                
                ambiguities.append({
                    'type': 'missing_dimensions',
                    'field': 'dimensions',
                    'context': 'Query mentions grouping but no dimensions specified',
                    'suggestions': available_dimensions[:5]  # Top 5 matches
                })
        
        # 5. Check for ambiguous filters
        if intent:
            filters = intent.get('filters', [])
            
            # Check if query has filter-like keywords but no filters extracted
            filter_keywords = ['where', 'filter', 'only', 'excluding', 'except', 'not']
            has_filter_keyword = any(kw in query_lower for kw in filter_keywords)
            
            if has_filter_keyword and not filters:
                ambiguities.append({
                    'type': 'ambiguous_filters',
                    'field': 'filters',
                    'context': 'Query mentions filtering but no filters extracted',
                    'query_text': query
                })
        
        return ambiguities
    
    def _generate_questions(self, ambiguities: List[Dict[str, Any]], 
                           query: str, metadata: Dict[str, Any]) -> List[ClarificationQuestion]:
        """
        Generate clarification questions from ambiguities.
        
        Uses LLM if available, otherwise uses rule-based generation.
        """
        questions = []
        
        # Use LLM if available for more natural questions
        if self.llm_provider:
            try:
                llm_questions = self._generate_questions_with_llm(ambiguities, query, metadata)
                questions.extend(llm_questions)
            except Exception as e:
                logger.warning(f"LLM question generation failed: {e}, using rule-based")
                questions.extend(self._generate_questions_rule_based(ambiguities, query, metadata))
        else:
            questions.extend(self._generate_questions_rule_based(ambiguities, query, metadata))
        
        return questions
    
    def _generate_questions_with_llm(self, ambiguities: List[Dict[str, Any]],
                                    query: str, metadata: Dict[str, Any]) -> List[ClarificationQuestion]:
        """Generate questions using LLM for more natural phrasing."""
        prompt = f"""You are a helpful assistant that asks clarifying questions to understand user queries better.

User Query: "{query}"

Ambiguities Detected:
{json.dumps(ambiguities, indent=2)}

Available Context:
- Tables: {len(metadata.get('tables', {}).get('tables', []))} tables available
- Metrics: {len(metadata.get('semantic_registry', {}).get('metrics', []))} metrics available
- Dimensions: {len(metadata.get('semantic_registry', {}).get('dimensions', []))} dimensions available

Generate 1-2 clarifying questions that are:
1. Natural and conversational (not technical)
2. Specific to what's missing
3. Offer options when available
4. Help the user clarify their intent

Return JSON array with questions:
[
  {{
    "question": "Natural question text",
    "context": "Why this question is needed",
    "field": "field_name",
    "options": ["option1", "option2"] // if applicable
  }}
]"""

        try:
            response = self.llm_provider.call_llm(prompt)
            
            # Parse JSON response
            if response.startswith("```json"):
                response = response[7:]
            if response.startswith("```"):
                response = response[3:]
            if response.endswith("```"):
                response = response[:-3]
            
            questions_data = json.loads(response.strip())
            
            return [
                ClarificationQuestion(
                    question=q.get('question', ''),
                    context=q.get('context', ''),
                    field=q.get('field', ''),
                    options=q.get('options'),
                    required=q.get('required', True)
                )
                for q in questions_data
            ]
        except Exception as e:
            logger.error(f"Error generating LLM questions: {e}")
            return self._generate_questions_rule_based(ambiguities, query, metadata)
    
    def _generate_questions_rule_based(self, ambiguities: List[Dict[str, Any]],
                                      query: str, metadata: Dict[str, Any]) -> List[ClarificationQuestion]:
        """Generate questions using rule-based templates."""
        questions = []
        
        for amb in ambiguities:
            amb_type = amb.get('type')
            
            if amb_type == 'missing_metric':
                options = amb.get('query_hints', [])
                question_text = "What metric would you like to see?"
                if options:
                    question_text += f" (e.g., {', '.join(options[:3])})"
                
                questions.append(ClarificationQuestion(
                    question=question_text,
                    context=amb.get('context', ''),
                    field='metric',
                    options=self._get_available_metrics(metadata)
                ))
            
            elif amb_type == 'ambiguous_table':
                questions.append(ClarificationQuestion(
                    question=f"Which table do you want to query? ({', '.join(amb.get('options', [])[:3])})",
                    context=amb.get('context', ''),
                    field='base_table',
                    options=amb.get('options', [])
                ))
            
            elif amb_type == 'missing_table':
                available = amb.get('available_tables', [])
                questions.append(ClarificationQuestion(
                    question="Which table or data do you want to query?",
                    context=amb.get('context', ''),
                    field='base_table',
                    options=available[:10]  # Limit options
                ))
            
            elif amb_type == 'missing_time_range':
                suggestions = amb.get('suggestions', [])
                questions.append(ClarificationQuestion(
                    question=f"What time period do you want? (e.g., {', '.join(suggestions[:2])})",
                    context=amb.get('context', ''),
                    field='time_range',
                    options=suggestions
                ))
            
            elif amb_type == 'missing_dimensions':
                suggestions = amb.get('suggestions', [])
                question_text = "How would you like to group the results?"
                if suggestions:
                    question_text += f" (e.g., {', '.join(suggestions[:3])})"
                
                questions.append(ClarificationQuestion(
                    question=question_text,
                    context=amb.get('context', ''),
                    field='dimensions',
                    options=suggestions
                ))
            
            elif amb_type == 'ambiguous_filters':
                questions.append(ClarificationQuestion(
                    question="What filters or conditions do you want to apply?",
                    context=amb.get('context', ''),
                    field='filters',
                    required=False  # Filters are optional
                ))
        
        return questions
    
    def _get_available_metrics(self, metadata: Dict[str, Any]) -> List[str]:
        """Get list of available metrics."""
        semantic_registry = metadata.get('semantic_registry', {})
        metrics = semantic_registry.get('metrics', [])
        return [m.get('name') for m in metrics if m.get('name')][:10]  # Limit to 10
    
    def generate_clarification_response(self, query: str, intent: Optional[Dict[str, Any]] = None,
                                      metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Generate a complete clarification response for API.
        
        Returns:
            Dictionary with clarification response structure
        """
        result = self.analyze_query(query, intent, metadata)
        
        response = {
            'success': False,  # Not successful yet - needs clarification
            'needs_clarification': result.needs_clarification,
            'confidence': result.confidence,
            'query': query
        }
        
        if result.needs_clarification:
            response['clarification'] = {
                'questions': [q.to_dict() for q in result.questions],
                'message': f"I need a bit more information to understand your query. Please answer these {len(result.questions)} question(s):"
            }
            
            if result.suggested_intent:
                response['suggested_intent'] = result.suggested_intent
                response['clarification']['message'] += " (Or I can proceed with my best guess if you'd like)"
        else:
            response['success'] = True
            response['intent'] = result.suggested_intent
        
        return response

