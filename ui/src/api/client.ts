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

