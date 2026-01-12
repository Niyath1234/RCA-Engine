import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080';

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Pipeline API
export const pipelineAPI = {
  list: () => apiClient.get('/api/pipelines'),
  create: (pipeline: any) => apiClient.post('/api/pipelines', pipeline),
  update: (id: string, pipeline: any) => apiClient.put(`/api/pipelines/${id}`, pipeline),
  delete: (id: string) => apiClient.delete(`/api/pipelines/${id}`),
  run: (id: string) => apiClient.post(`/api/pipelines/${id}/run`),
  status: (id: string) => apiClient.get(`/api/pipelines/${id}/status`),
};

// Reasoning API
export const reasoningAPI = {
  query: (query: string, context?: any) =>
    apiClient.post('/api/reasoning/query', { query, context }),
  stream: (query: string) => {
    // For streaming responses
    return fetch(`${API_BASE_URL}/api/reasoning/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
  },
};

// Ingestion API
export const ingestionAPI = {
  ingest: (config: any) => apiClient.post('/api/ingestion/ingest', config),
  validate: (config: any) => apiClient.post('/api/ingestion/validate', config),
  preview: (config: any) => apiClient.post('/api/ingestion/preview', config),
  uploadCsv: (formData: FormData) => {
    return apiClient.post('/api/upload/csv', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
  },
};

// Rules API
export const rulesAPI = {
  list: () => apiClient.get('/api/rules'),
  get: (id: string) => apiClient.get(`/api/rules/${id}`),
  create: (rule: any) => apiClient.post('/api/rules', rule),
  update: (id: string, rule: any) => apiClient.put(`/api/rules/${id}`, rule),
  delete: (id: string) => apiClient.delete(`/api/rules/${id}`),
};

