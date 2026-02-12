import React, { useState, useEffect } from "react";

const Panel = ({ className }) => {
  return (
    <div className={`${className} flex flex-col gap-2`}>
      <div className="grow flex flex-row gap-2">
        <div className="grow flex flex-col items-center overflow-y-auto border-2 border-black rounded-lg min-h-0">
          {/* <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div> */}
        </div>

        {/* <div className="grow flex flex-col items-center overflow-x-auto border-2 border-black rounded-lg min-h-0">
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
          <div className="">AAAA</div>
        </div> */}
      </div>

      <div className="basis-20 flex flex-row gap-2">
        <input
          id="base"
          type="text"
          // placeholder={placeholder}
          // value={wordEntered}
          // onChange={handleFilter}
          className="w-1/2 text-3xl flex flex-row items-center rounded-md hover:bg-gray-100 focus-visible:bg-gray-200 "
        />
        <button className="grow mb-4 border-2 border-black rounded-lg hover:bg-gray-300">
          A
        </button>
        <button className="grow mb-4 border-2 border-black rounded-lg hover:bg-gray-300">
          B
        </button>

        <button className="grow mb-4 border-2 border-black rounded-lg hover:bg-gray-300">
          X
        </button>
        <button className="grow mb-4 border-2 border-black rounded-lg hover:bg-gray-300">
          Y
        </button>
      </div>
    </div>
  );
};

export default Panel;
