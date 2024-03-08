## Chat Design

Building a real-time messaging platform without a dedicated server.

### Description

This chat is a single channel, where multiple people can write and read messages.

#### Message.index

```typescript
{
  username: string;
  message_id: number;
  timestamp: number;
  content: string;
}
```

We can query for messages sent by a specific user or by a specific date or time.
