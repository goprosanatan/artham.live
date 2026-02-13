import { getDPR } from "@components/Chart/__Common.js";

export function createShader(gl, type, src) {
  const sh = gl.createShader(type);
  gl.shaderSource(sh, src);
  gl.compileShader(sh);
  if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
    console.error(gl.getShaderInfoLog(sh));
    gl.deleteShader(sh);
    return null;
  }
  return sh;
}

export function createProgram(gl, vsSrc, fsSrc) {
  const vs = createShader(gl, gl.VERTEX_SHADER, vsSrc);
  const fs = createShader(gl, gl.FRAGMENT_SHADER, fsSrc);
  const prog = gl.createProgram();
  gl.attachShader(prog, vs);
  gl.attachShader(prog, fs);
  gl.linkProgram(prog);
  if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
    console.error(gl.getProgramInfoLog(prog));
    gl.deleteProgram(prog);
    return null;
  }
  gl.deleteShader(vs);
  gl.deleteShader(fs);
  return prog;
}

export function createGLResources(gl) {
  const vsSrc = `#version 300 es
    precision highp float;
    in vec2 aPos;
    in vec2 iCenter;
    in vec2 iHalfSize;
    in vec4 iColor;
    uniform vec2 uCanvas;
    out vec4 vColor;
    void main() {
      vec2 posPx = iCenter + aPos * iHalfSize * 2.0;
      vec2 clip = (posPx / uCanvas) * 2.0 - 1.0;
      gl_Position = vec4(clip.x, -clip.y, 0.0, 1.0);
      vColor = iColor;
    }`;
  const fsSrc = `#version 300 es
    precision mediump float;
    in vec4 vColor;
    out vec4 outColor;
    void main() { outColor = vColor; }`;

  const program = createProgram(gl, vsSrc, fsSrc);
  const aPos = gl.getAttribLocation(program, "aPos");
  const iCenter = gl.getAttribLocation(program, "iCenter");
  const iHalfSize = gl.getAttribLocation(program, "iHalfSize");
  const iColor = gl.getAttribLocation(program, "iColor");
  const uCanvas = gl.getUniformLocation(program, "uCanvas");

  const quad = new Float32Array([
    -0.5, -0.5, 0.5, -0.5, -0.5, 0.5, -0.5, 0.5, 0.5, -0.5, 0.5, 0.5,
  ]);
  const quadVBO = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, quadVBO);
  gl.bufferData(gl.ARRAY_BUFFER, quad, gl.STATIC_DRAW);

  const instCenter = gl.createBuffer();
  const instHalf = gl.createBuffer();
  const instColor = gl.createBuffer();
  const vao = gl.createVertexArray();

  return {
    program,
    aPos,
    iCenter,
    iHalfSize,
    iColor,
    uCanvas,
    quadVBO,
    instCenter,
    instHalf,
    instColor,
    maxInstances: 300000,
    vao,
  };
}

/**
 * Ensure a WebGL2 context and base resources exist and are bound to the refs.
 */
export function ensureGL(canvas, w, h, glRef, glResRef, vaoRef) {
  const dpr = getDPR();
  const fbW = Math.floor(w * dpr);
  const fbH = Math.floor(h * dpr);
  canvas.width = fbW;
  canvas.height = fbH;
  canvas.style.width = w + "px";
  canvas.style.height = h + "px";

  let gl = glRef.current;
  if (!gl) {
    gl = canvas.getContext("webgl2", {
      antialias: false,
      depth: false,
      stencil: false,
      premultipliedAlpha: true,
    });
    if (!gl) return null;

    glRef.current = gl;
    gl.enable(gl.BLEND);
    gl.blendFuncSeparate(
      gl.SRC_ALPHA,
      gl.ONE_MINUS_SRC_ALPHA,
      gl.ONE,
      gl.ONE_MINUS_SRC_ALPHA
    );

    glResRef.current = createGLResources(gl);
  }

  gl.viewport(0, 0, fbW, fbH);

  if (!vaoRef.current) {
    const { vao, aPos, quadVBO, program } = glResRef.current;
    vaoRef.current = vao;
    gl.bindVertexArray(vao);
    gl.useProgram(program);
    gl.bindBuffer(gl.ARRAY_BUFFER, quadVBO);
    gl.enableVertexAttribArray(aPos);
    gl.vertexAttribPointer(aPos, 2, gl.FLOAT, false, 0, 0);
    gl.bindVertexArray(null);
  }

  return gl;
}

/**
 * Helper to bind an instanced attribute.
 */
export function bindInst(gl, buffer, attrib, size) {
  gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
  gl.enableVertexAttribArray(attrib);
  gl.vertexAttribPointer(attrib, size, gl.FLOAT, false, 0, 0);
  gl.vertexAttribDivisor(attrib, 1);
}

/**
 * Optional helper to upload and draw a batch with a single call.
 */
export function drawInstancedBatch(
  gl,
  res,
  vao,
  centers,
  halves,
  colors,
  count
) {
  gl.bindVertexArray(vao);

  gl.bindBuffer(gl.ARRAY_BUFFER, res.instCenter);
  gl.bufferData(
    gl.ARRAY_BUFFER,
    centers.subarray(0, count * 2),
    gl.DYNAMIC_DRAW
  );
  bindInst(gl, res.instCenter, res.iCenter, 2);

  gl.bindBuffer(gl.ARRAY_BUFFER, res.instHalf);
  gl.bufferData(
    gl.ARRAY_BUFFER,
    halves.subarray(0, count * 2),
    gl.DYNAMIC_DRAW
  );
  bindInst(gl, res.instHalf, res.iHalfSize, 2);

  gl.bindBuffer(gl.ARRAY_BUFFER, res.instColor);
  gl.bufferData(
    gl.ARRAY_BUFFER,
    colors.subarray(0, count * 4),
    gl.DYNAMIC_DRAW
  );
  bindInst(gl, res.instColor, res.iColor, 4);

  gl.drawArraysInstanced(gl.TRIANGLES, 0, 6, count);
  gl.bindVertexArray(null);
}
