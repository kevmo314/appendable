import { Component, onMount } from "solid-js";

const Header: Component<{ username: string | null }> = ({ username }) => {
  const headerContent = [
    {
      title: "Appendable",
      link: "https://github.com/kevmo314/appendable",
    },
    {
      title: "Source Code",
      link: "https://github.com/kevmo314/appendable/tree/main/examples/chat#readme",
    },
  ];

  return (
    <header class="w-full items-center h-10 flex justify-between px-8">
      <div class="flex space-x-8">
        {headerContent.map(({ title, link }) => {
          return (
            <a
              class="hover:underline underline-offset-4"
              id={title}
              href={link}
              target="blank"
            >
              <p>{title}</p>
            </a>
          );
        })}
      </div>

      <p class="font-semibold">{username}</p>
    </header>
  );
};

export default Header;
