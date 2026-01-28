import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080';

export interface AgentResponse {
  status: string;
  message?: string;
  choices?: Array<{ id: string; label: string; description?: string }>;
  error?: string;
  data?: any;
  trace?: Array<{
    event_type: string;
    payload?: any;
  }>;
  clarification?: ClarificationRequest;
  final_answer?: string;
}

export interface ClarificationRequest {
  query: string;
  answer?: string;
  question?: string;
  confidence?: number;
  partial_understanding?: {
    task_type?: string;
    metrics?: string[];
    systems?: string[];
  };
  missing_pieces?: Array<{
    field: string;
    importance: string;
    description: string;
    suggestions?: string[];
  }>;
  response_hints?: string[];
  choices?: Array<{ id: string; label: string; score?: number }>;
}

export const agentAPI = {
  run: async (sessionId: string, query: string, uiContext?: any): Promise<AgentResponse> => {
    const response = await axios.post(`${API_BASE_URL}/api/agent/run`, {
      session_id: sessionId,
      user_query: query,
      ui_context: uiContext || {},
    });
    return response.data;
  },
  continue: async (sessionId: string, choiceId: string, uiContext?: any): Promise<AgentResponse> => {
    const response = await axios.post(`${API_BASE_URL}/api/agent/continue`, {
      session_id: sessionId,
      choice_id: choiceId,
      ui_context: uiContext || {},
    });
    return response.data;
  },
};

export const reasoningAPI = {
  assess: async (query: string): Promise<any> => {
    const response = await axios.post(`${API_BASE_URL}/api/reasoning/assess`, {
      query,
    });
    return response.data;
  },
  clarify: async (query: string, answer: string): Promise<any> => {
    const response = await axios.post(`${API_BASE_URL}/api/reasoning/clarify`, {
      query,
      answer,
    });
    return response.data;
  },
  query: async (query: string): Promise<any> => {
    const response = await axios.post(`${API_BASE_URL}/api/reasoning/query`, {
      query,
    });
    return response.data;
  },
};

export const assistantAPI = {
  ask: async (question: string): Promise<any> => {
    const response = await axios.post(`${API_BASE_URL}/api/assistant/ask`, {
      question,
    });
    return response.data;
  },
};

export interface QueryGenerationResult {
  success: boolean;
  sql?: string;
  metric?: {
    name: string;
    description: string;
  } | null;
  dimensions?: Array<{
    name: string;
    description: string;
  }>;
  joins?: Array<{
    from_table: string;
    to_table: string;
    on: string;
  }>;
  filters?: string[] | Array<Record<string, any>>;
  error?: string;
  suggestion?: string;
  business_rules_applied?: string[];
  reasoning_steps?: string[];
  method?: string;
  intent?: Record<string, any>;
}

export interface PrerequisitesResult {
  success: boolean;
  metadata?: {
    semantic_registry: any;
    tables: any;
  };
  loaded?: {
    metrics: number;
    dimensions: number;
    tables: number;
  };
  error?: string;
}

export const queryAPI = {
  loadPrerequisites: async (): Promise<PrerequisitesResult> => {
    const response = await axios.get(`${API_BASE_URL}/api/query/load-prerequisites`);
    return response.data;
  },
  generateSQL: async (query: string, useLLM: boolean = true): Promise<QueryGenerationResult> => {
    const response = await axios.post(`${API_BASE_URL}/api/query/generate-sql`, {
      query,
      use_llm: useLLM,
    });
    return response.data;
  },
};

