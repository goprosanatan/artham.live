import React, { useState, useEffect } from "react";

const LiveClock = ({className}) => {
  const [currentTime, setCurrentTime] = useState(new Date());

  useEffect(() => {
    const timerId = setInterval(() => {
      setCurrentTime(new Date());
    }, 1000); // Update every 1000 milliseconds (1 second)

    // Clear the interval when the component unmounts to prevent memory leaks
    return () => clearInterval(timerId);
  }, []); // Empty dependency array ensures this effect runs only once on mount

  return (
    <p className={`${className}`}>
      {currentTime.toDateString()} - {currentTime.toLocaleTimeString()}
    </p>
  );
};

export default LiveClock;
