import { Component, type Resource } from "solid-js";
import { Message } from "../App";

type ChannelProps = {
  messages: Resource<Message[]>;
};

const Channel: Component<ChannelProps> = ({ messages }) => {
  function readTime(unixDate: number): string {
    return new Date(unixDate).toLocaleString();
  }

  return (
    <div class="flex-1">
      {messages.loading && <p>Loading...</p>}
      {messages.error && <p>Error loading messages</p>}
      {messages()?.map((message) => {
        return (
          <div>
            <p>
              {message.content} - {readTime(message.timestamp)}
            </p>
          </div>
        );
      })}
    </div>
  );
};

export default Channel;
