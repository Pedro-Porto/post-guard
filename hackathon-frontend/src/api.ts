// src/api.ts
import type { PostType, PostDetailsType } from './types';

// Definimos a URL base da nossa API em um só lugar
const API_BASE_URL = 'http://andromeda.lasdpc.icmc.usp.br:60105';

// --- FUNÇÃO PARA BUSCAR A LISTA INICIAL DE POSTS ---
// (Substitua '/posts' pelo seu endpoint real para buscar a lista)
export const fetchAllPosts = async (): Promise<PostType[]> => {
  const response = await fetch(`${API_BASE_URL}/posts`);
  if (!response.ok) {
    throw new Error('Falha ao buscar os posts do servidor.');
  }
  return response.json();
};

// --- FUNÇÃO PARA CRIAR UM NOVO POST (POST) ---
// (Substitua '/posts' pelo seu endpoint real de criação)
// A função createPost agora envia SOMENTE o texto, como na imagem.
export const createPost = async (text: string): Promise<PostType> => {
  const response = await fetch(`http://andromeda.lasdpc.icmc.usp.br:60105/posts`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    // O corpo da requisição agora envia apenas o campo "text"
    body: JSON.stringify({
      text: text,
    }),
  });

  if (!response.ok) {
    throw new Error('Falha ao criar o post.');
  }
  
  // A API deve responder com o post completo recém-criado
  return response.json();
};



// --- FUNÇÃO PARA BUSCAR DETALHES DE UM POST (GET) ---
// (Substitua '/posts/${postId}/details' pelo seu endpoint real de detalhes)
export const fetchPostDetails = async (postId: number): Promise<PostDetailsType> => {
  const response = await fetch(`${API_BASE_URL}/posts/${postId}/details`);

  if (!response.ok) {
    throw new Error(`Falha ao buscar detalhes para o post ${postId}.`);
  }
  
  return response.json();
};