import { RouterProvider, createBrowserRouter } from "react-router-dom";
import { useAuth } from "@contexts/authProvider.jsx";
import { ProtectedRoute } from "@routes/ProtectedRoute.jsx";
import Login from "@pages/Login.jsx";
import Dashboard from "@pages/Dashboard.jsx";
import Replay from "@pages/Replay.jsx";
import AboutUs from "@pages/AboutUs.jsx";
import Page404 from "@pages/Page404.jsx";

const AllRoutes = () => {
  const { token } = useAuth();

  // Define public routes accessible to all users
  const routesForPublic = [
    {
      path: "/",
      element: <Login />,
    },
    {
      path: "*",
      element: <Page404 />,
    },
  ];

  // Define routes accessible only to non-authenticated users
  const routesForNotAuthenticatedOnly = [
    {
      path: "/",
      element: <Login />,
    },
  ];

  // Define routes accessible only to authenticated users
  const routesForAuthenticatedOnly = [
    {
      path: "/",
      element: <ProtectedRoute />, // Wrap the component in ProtectedRoute
      children: [
        {
          path: "dashboard",
          element: <Dashboard />,
        },
        {
          path: "replay",
          element: <Replay />,
        },
        {
          path: "aboutus",
          element: <AboutUs />,
        },
      ],
    },
  ];

  // Combine and conditionally include routes based on authentication status
  const router = createBrowserRouter(
    [
      ...routesForPublic,
      ...(!token ? routesForNotAuthenticatedOnly : []),
      ...routesForAuthenticatedOnly,
    ],
    // for react warnings
    {
      future: {
        v7_fetcherPersist: true,
        v7_normalizeFormMethod: true,
        v7_partialHydration: true,
        v7_relativeSplatPath: true,
        v7_skipActionErrorRevalidation: true,
        v7_startTransition: true,
      },
    }
  );

  // Provide the router configuration using RouterProvider
  return (
    <RouterProvider future={{ v7_startTransition: true }} router={router} />
  );
};

export default AllRoutes;
