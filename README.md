# OnceFlow

O **OnceFlow** é uma biblioteca em Go projetada para gerenciar a execução de operações idempotentes. Ele garante que tarefas críticas (como o processamento de pagamentos ou chamadas de API duplicadas) não sejam executadas múltiplas vezes simultaneamente em ambientes distribuídos.

## 🚀 Passo a Passo de Instalação

Siga as etapas abaixo para instalar e começar a usar o `onceflow` no seu projeto Go:

### 1. Pré-requisitos
* **Go 1.21** ou superior instalado na sua máquina.
* Um banco de dados para gerenciar os *locks* distribuídos (ex: **Redis**).

### 2. Baixar a Biblioteca
No diretório do seu projeto Go, instale o pacote principal do `onceflow` executando o comando abaixo:

```bash
go get github.com/seu-usuario/onceflow
```

*(Observação: Se o repositório estiver em outro domínio/usuário, substitua a URL correspondente)*.

### 3. Instalar as Dependências de Armazenamento
O `onceflow` precisa de um *adapter* de armazenamento para funcionar. Se for utilizar o **Redis** (que é suportado nativamente via `adapter/redisadapter`), você precisará adicionar o driver do Redis ao seu projeto:

```bash
go get github.com/redis/go-redis/v9
```

### 4. Exemplo Rápido de Configuração e Uso
Após a instalação, você já pode importar a biblioteca e configurá-la.

```go
package main

import (
	"context"
	"fmt"
	"time"
	
	"github.com/redis/go-redis/v9"
	"github.com/seu-usuario/onceflow"
	"github.com/seu-usuario/onceflow/adapter/redisadapter"
)

func main() {
	// 1. Configurar o cliente Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	// 2. Instanciar o adapter de armazenamento do Redis
	storage := redisadapter.New(rdb)

	// 3. Inicializar o Engine do OnceFlow
	engine := onceflow.New(onceflow.Config{
		Storage:      storage,
		LockTTL:      30 * time.Second, // Tempo de bloqueio caso o processo falhe antes do fim
		RetentionTTL: 24 * time.Hour,   // Tempo em que o resultado ficará gravado no Redis
	})

	ctx := context.Background()
	chaveOperacao := "pedido_123"

	// 4. Tentar adquirir a exclusividade da operação
	resultado, err := engine.Acquire(ctx, chaveOperacao)
	if err != nil {
		if err == onceflow.ErrConflict {
			fmt.Println("Erro: A operação já está em andamento!")
			return
		}
		panic(err)
	}
	
	// Se Status for Completed, a operação já havia sido finalizada com sucesso anteriormente.
	// Podemos simplesmente retornar os dados cacheados!
	if resultado.Status == onceflow.StatusCompleted {
		fmt.Printf("Operação já realizada! Resultado em cache: %s\n", resultado.Body)
		return
	}

	// 5. Caso contrário, executar a lógica principal (ex: processar pagamento)
	fmt.Println("Processando a operação pela primeira vez...")

	// 6. Após o processamento, resolver (salvar) o estado no OnceFlow
	err = engine.Resolve(ctx, chaveOperacao, onceflow.Result{
		Status:     onceflow.StatusCompleted,
		StatusCode: 200,
		Body:       `{"mensagem": "sucesso"}`,
	})
	
	if err != nil {
		fmt.Println("Falha ao persistir o resultado:", err)
	}
}
```

### Uso com PostgreSQL

Caso você não queira utilizar o Redis, o OnceFlow também possui suporte nativo para **PostgreSQL**. Ele utiliza o conceito de exclusividade atômica (`ON CONFLICT DO NOTHING`) e gerencia a validade do lock e a expiração do cache através do campo `expires_at`.

Para utilizar, primeiro crie a tabela no seu banco de dados:

```sql
CREATE TABLE IF NOT EXISTS onceflow_keys (
    key VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    status_code INT,
    headers TEXT,
    body TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL
);
```

Na sua aplicação, inicialize utilizando o `postgresadapter`:

```go
import (
	"database/sql"
	_ "github.com/lib/pq"
	"github.com/seu-usuario/onceflow/adapter/postgresadapter"
)

// ...

db, err := sql.Open("postgres", "postgres://user:pass@localhost/db?sslmode=disable")

// Passa a conexão do BD para o adapter
storage := postgresadapter.New(db)

engine := onceflow.New(onceflow.Config{
    Storage:      storage,
    LockTTL:      1 * time.Minute, // O lock será invalidado via expires_at após 1 min
    RetentionTTL: 5 * time.Minute, // A resposta cacheada será invalidada via expires_at após 5 min
})
```


## 📂 Estrutura

- `engine.go`: Contém a lógica principal do `OnceFlow` (métodos `Acquire`, `Resolve`, `Fail`).
- `types.go`: Define os estados e estruturas de resultado esperadas.
- `adapter/`: Contém as implementações dos armazenamentos externos (Redis, PostgreSQL, etc).
- `middleware/`: Middleware prontos para uso (se houver integração com frameworks HTTP).