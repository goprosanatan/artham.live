import { Blockquote, Button } from "flowbite-react";
import { IoFlag } from "react-icons/io5";

export function Page404() {
  return (
    <div className="h-screen mx-auto grid place-items-center text-center px-8 dark:bg-gray-800">
      <div>
        <IoFlag className="w-20 h-20 mx-auto" />
        <Blockquote
          variant="h1"
          color="blue-gray"
          className="mt-10 !text-3xl !leading-snug md:!text-4xl dark:text-white"
        >
          Error 404 <br /> It looks like something went wrong.
        </Blockquote>
        <Blockquote className="mt-8 mb-14 text-[18px] font-normal text-gray-500 mx-auto md:max-w-sm  dark:text-white">
          Don&apos;t worry, our team is already on it. Please try refreshing the page or come back later.
        </Blockquote>
        <Button color="light" className="w-full mx-auto px-4 mt-4 text-black hover:bg-gray-200 dark:hover:bg-gray-400 dark:text-white" href="/">
          Go to Home!
        </Button>
      </div>
    </div>
  );
}

export default Page404;
