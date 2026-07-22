import { createServer } from 'node:http'

createServer((_request, response) => {
  response.writeHead(200, { 'content-type': 'text/plain' })
  response.end('factory-cloudflare-container-ok')
}).listen(8080, '0.0.0.0')
