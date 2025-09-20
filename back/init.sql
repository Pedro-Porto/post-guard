DO $$ 
BEGIN
    -- Tabela posts (se ainda não existir)
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'posts') THEN
        CREATE TABLE posts (
            id VARCHAR(36) PRIMARY KEY,
            text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_posts_created_at ON posts(created_at);
    END IF;

    -- Tabela rag_results
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rag_results') THEN
        CREATE TABLE rag_results (
            id SERIAL PRIMARY KEY,
            job_id VARCHAR(100) NOT NULL,  -- Nova coluna adicionada
            post_id VARCHAR(36) NOT NULL,
            version VARCHAR(50) NOT NULL,
            stance VARCHAR(50) NOT NULL,
            decision VARCHAR(100) NOT NULL,
            confidence FLOAT NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
            scores JSON NOT NULL,
            explanation TEXT NOT NULL,
            citations JSON NOT NULL,
            latency_ms INTEGER NOT NULL CHECK (latency_ms >= 0),
            model_used VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
        );
        
        -- Criar índices
        CREATE INDEX idx_rag_results_job_id ON rag_results(job_id);
        CREATE INDEX idx_rag_results_post_id ON rag_results(post_id);
        CREATE INDEX idx_rag_results_created_at ON rag_results(created_at);
        CREATE INDEX idx_rag_results_stance ON rag_results(stance);
        CREATE INDEX idx_rag_results_decision ON rag_results(decision);
        
        -- Adicionar comentários às colunas para documentação
        COMMENT ON TABLE rag_results IS 'Tabela para armazenar resultados de processamento RAG';
        COMMENT ON COLUMN rag_results.job_id IS 'ID do job de processamento';
        COMMENT ON COLUMN rag_results.post_id IS 'ID do post analisado (FK para posts.id)';
        COMMENT ON COLUMN rag_results.version IS 'Versão do modelo/processamento';
        COMMENT ON COLUMN rag_results.stance IS 'Posição/opinião detectada';
        COMMENT ON COLUMN rag_results.decision IS 'Decisão tomada com base na análise';
        COMMENT ON COLUMN rag_results.confidence IS 'Nível de confiança da análise (0.0 a 1.0)';
        COMMENT ON COLUMN rag_results.scores IS 'Scores detalhados em formato JSON';
        COMMENT ON COLUMN rag_results.explanation IS 'Explicação textual da decisão';
        COMMENT ON COLUMN rag_results.citations IS 'Fontes/citações usadas na análise';
        COMMENT ON COLUMN rag_results.latency_ms IS 'Tempo de processamento em milissegundos';
        COMMENT ON COLUMN rag_results.model_used IS 'Modelo utilizado para a análise';
        
    ELSE
        -- Se a tabela já existe, verificar se a coluna job_id existe
        IF NOT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_name = 'rag_results' 
            AND column_name = 'job_id'
        ) THEN
            -- Adicionar a coluna job_id se não existir
            ALTER TABLE rag_results 
            ADD COLUMN job_id VARCHAR(100) NOT NULL DEFAULT 'unknown_job';
            
            -- Criar índice para a nova coluna
            CREATE INDEX idx_rag_results_job_id ON rag_results(job_id);
            
            -- Atualizar constraint para não usar mais DEFAULT
            ALTER TABLE rag_results 
            ALTER COLUMN job_id DROP DEFAULT;
        END IF;
    END IF;
END $$;